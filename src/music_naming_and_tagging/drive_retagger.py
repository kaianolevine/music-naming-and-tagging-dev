from __future__ import annotations

import os
import re
import tempfile
import unicodedata
from typing import Any, Dict, Tuple

# import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as drive
import kaiano_common_utils.logger as log
import music_tag
from mutagen.id3 import ID3, TDRC, TYER, ID3NoHeaderError

from music_naming_and_tagging.retagger import (
    AcoustIdIdentifier,
    MusicBrainzRecordingProvider,
    MusicTagIO,
    TagSnapshot,
    TrackMetadata,
)

TAG_FIELDS = [
    # music-tag keys
    "tracktitle",
    "artist",
    "album",
    "albumartist",
    "year",
    "date",
    "genre",
    "bpm",
    "comment",
    "isrc",
    "tracknumber",
    "discnumber",
]


def _safe_str(v: Any) -> str:
    """Best-effort stringify without turning missing values into the literal 'None'."""
    if v is None:
        return ""
    try:
        s = str(v)
    except Exception:
        return ""
    # Some tag wrappers stringify missing values as "None"
    if s.strip().lower() == "none":
        return ""
    return s


def _title_case_words(v: Any) -> str:
    """
    Capitalize every word that starts with a letter.
    Preserves existing punctuation and spacing.
    """
    s = _safe_str(v)
    if not s:
        return ""

    def repl(match: re.Match) -> str:
        word = match.group(0)
        return word[0].upper() + word[1:]

    # Capitalize words that start with an alphabetic character
    return re.sub(r"\b[a-zA-Z][^\s]*", repl, s)


def _normalize_for_compare(v: Any) -> str:
    """Canonical comparison: None / 'None' / whitespace all become empty string."""
    return _safe_str(v).strip()


def _normalize_year_for_tag(v: Any) -> str:
    s = _safe_str(v).strip()
    if not s:
        return ""
    if len(s) >= 4 and s[:4].isdigit():
        return s[:4]
    return ""


def _safe_filename_component(v: Any) -> str:
    """
    Normalize a value for safe, deterministic filenames.

    Rules:
    - Convert to string
    - Strip accents / diacritics
    - Lowercase
    - Remove all whitespace
    - Remove all non-alphanumeric characters (except underscore)
    - Collapse multiple underscores
    """
    s = _safe_str(v)

    if not s:
        return ""

    # Normalize unicode (e.g. Beyoncé -> Beyonce)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))

    s = s.lower()

    # Remove whitespace entirely
    s = re.sub(r"\s+", "", s)

    # Replace any remaining invalid chars with underscore
    s = re.sub(r"[^a-z0-9_]", "_", s)

    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)

    return s.strip("_")


def _delete_drive_file(service: Any, file_id: str) -> None:
    """
    Permanently delete a file from Google Drive.
    Only call this after a successful end-to-end process.
    """
    service.files().delete(fileId=file_id, supportsAllDrives=True).execute()


def _print_all_tags(path: str) -> None:
    try:
        f = music_tag.load_file(path)
    except Exception as e:
        log.error(f"[TAGS-ERROR] Failed to read tags for {os.path.basename(path)}: {e}")
        return

    # Try to enumerate everything music-tag knows about.
    # Not all backends expose a stable tag-key list, so we print a curated set + anything else accessible.
    printed: Dict[str, str] = {}

    for k in TAG_FIELDS:
        try:
            v = f[k]
            if k == "artwork":
                continue
            printed[k] = _safe_str(v)
        except Exception:
            printed[k] = ""

    # Attempt to discover additional keys if available
    extra_keys = []
    try:
        extra_keys = [
            k for k in getattr(f, "keys")() if k not in printed and k != "artwork"
        ]
    except Exception:
        extra_keys = []

    for k in sorted(extra_keys):
        try:
            printed[k] = _safe_str(f[k])
        except Exception:
            continue

    # Log in a stable order
    log.info(f"[FILE] {os.path.basename(path)}")
    for k in sorted(printed.keys()):
        v = printed[k]
        if v is None:
            v = ""
        log.info(f"  [TAG] {k} = {v}")


def _log_candidate_options(
    name: str, candidates: list[Any], *, max_show: int = 5
) -> None:
    if not candidates:
        log.info(f"[CANDIDATES] {name}: none")
        return

    top = sorted(candidates, key=lambda c: c.confidence, reverse=True)[:max_show]
    rendered = ", ".join(f"{c.provider}:{c.id}({c.confidence:.3f})" for c in top)
    log.info(f"[CANDIDATES] {name}: {rendered}")


def _build_updates_with_conflict_logging(
    existing: TagSnapshot, new_meta: TrackMetadata
) -> Tuple[TrackMetadata, bool]:
    """
    Overwrite existing tag values with new metadata when provided, except for genre (fill-only).
    """
    existing_genre = _normalize_for_compare(existing.tags.get("genre"))
    new_genre = _normalize_for_compare(new_meta.genre)
    genre_to_write = new_meta.genre if (not existing_genre and new_genre) else None

    existing_comment = _safe_str(existing.tags.get("comment")).strip()
    if existing_comment == "":
        comment_to_write = "<KAT_v1>"
    elif existing_comment.startswith("<KAT_v1>"):
        # Already tagged by this pipeline; keep as-is (avoid duplicate prefixes)
        comment_to_write = existing_comment
    else:
        comment_to_write = "<KAT_v1> " + existing_comment

    normalized_year = _normalize_year_for_tag(new_meta.year)

    updates = TrackMetadata(
        title=_title_case_words(new_meta.title),
        artist=_title_case_words(new_meta.artist),
        album=new_meta.album,
        album_artist=new_meta.album_artist,
        year=normalized_year,
        # Genre is fill-only. If we are not writing genre, keep it as None so we do not
        # overwrite an existing genre tag with an empty string.
        genre=_title_case_words(genre_to_write) if genre_to_write is not None else None,
        bpm=new_meta.bpm,
        comment=comment_to_write,
        isrc=new_meta.isrc,
        track_number=new_meta.track_number,
        disc_number=new_meta.disc_number,
        raw=new_meta.raw,
    )

    return updates, False


def process_drive_folder_for_retagging(
    source_folder_id: str,
    dest_folder_id: str,
    *,
    acoustid_api_key: str,
    min_confidence: float = 0.90,
    max_candidates: int = 5,
    max_uploads_per_run: int = 50,
) -> Dict[str, int]:
    """
    Orchestrates:
      - list audio files in source Drive folder
      - download each to temp
      - print filename + tags
      - identify via AcoustID -> MusicBrainz recording MBID
      - fetch metadata from MusicBrainz
      - write tags using music-tag (skip conflicting fields, log mismatches)
      - upload updated file to destination Drive folder

    Returns summary:
      {"scanned": int, "downloaded": int, "identified": int, "tagged": int, "uploaded": int, "failed": int}
    """
    service = drive.get_drive_service()

    identifier = AcoustIdIdentifier(
        api_key=acoustid_api_key,
        min_confidence=min_confidence,
        max_candidates=max_candidates,
    )
    provider = MusicBrainzRecordingProvider(
        app_name="music-naming-and-tagging",
        app_version="0.1.0",
        contact="https://example.com",
        throttle_s=1.0,
    )
    tag_io = MusicTagIO()

    summary = {
        "scanned": 0,
        "downloaded": 0,
        "identified": 0,
        "tagged": 0,
        "uploaded": 0,
        "failed": 0,
        "deleted": 0,
    }

    music_files = drive.list_music_files(service, source_folder_id)
    log.info(
        f"[START] Found {len(music_files)} music files in source folder (max uploads per run={max_uploads_per_run})."
    )

    for file in music_files:
        if (
            max_uploads_per_run
            and max_uploads_per_run > 0
            and summary["uploaded"] >= max_uploads_per_run
        ):
            log.info(
                f"[STOP] Reached max uploads per run ({max_uploads_per_run}). Stopping."
            )
            break

        summary["scanned"] += 1
        file_id = file.get("id")
        name = file.get("name", "unknown")
        temp_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{name}")

        try:
            log.info(f"[DOWNLOAD] {name} ({file_id}) -> {temp_path}")
            drive.download_file(service, file_id, temp_path)
            summary["downloaded"] += 1

            # Print existing tags
            log.info("[PRE-EXISTING-TAGS]------------------")
            _print_all_tags(temp_path)

            # Identify
            snapshot = tag_io.read(temp_path)
            candidates = list(identifier.identify(temp_path, snapshot))
            _log_candidate_options(name, candidates, max_show=max_candidates)

            if not candidates:
                log.info(
                    f"[IDENTIFY-SKIP] {name}: no candidates returned (continuing without tagging)"
                )
                continue

            chosen = max(candidates, key=lambda c: c.confidence)
            if chosen.confidence < min_confidence:
                log.info(
                    f"[IDENTIFY-SKIP] {name}: best score {chosen.confidence:.3f} below threshold {min_confidence:.2f} (continuing without tagging)"
                )
                continue

            summary["identified"] += 1
            log.info(
                f"[IDENTIFY] {name}: provider={chosen.provider} id={chosen.id} score={chosen.confidence:.3f}"
            )

            # Fetch metadata
            meta = provider.fetch(chosen)
            log.info(
                "[META] "
                + ", ".join(
                    f"{k}={v!r}"
                    for k, v in {
                        "title": meta.title,
                        "artist": meta.artist,
                        "album": meta.album,
                        "year": meta.year,
                        "genre": meta.genre,
                        "isrc": meta.isrc,
                    }.items()
                    if v
                )
            )

            # Conflict-aware updates
            updates, had_conflict = _build_updates_with_conflict_logging(snapshot, meta)

            # If everything conflicts / nothing to write, we still consider run successful and upload unchanged file.
            # Tagging is only counted if we wrote without raising.
            tag_io.write(temp_path, updates)

            # VirtualDJ has historically been more reliable reading ID3v2.3 TYER.
            # `music_tag` may write ID3v2.4 (TDRC/date). Here we best-effort ensure a
            # TYER frame exists and re-save as ID3v2.3 for compatibility.
            normalized_year = _normalize_year_for_tag(updates.year)
            if normalized_year:
                try:
                    try:
                        id3 = ID3(temp_path)
                    except ID3NoHeaderError:
                        id3 = ID3()

                    # Ensure both frames exist; VirtualDJ often uses TYER.
                    try:
                        id3.setall("TYER", [TYER(encoding=3, text=normalized_year)])
                    except Exception:
                        pass
                    try:
                        id3.setall("TDRC", [TDRC(encoding=3, text=normalized_year)])
                    except Exception:
                        pass

                    # Save as ID3v2.3 for maximum player compatibility
                    id3.save(temp_path, v2_version=3)
                except Exception:
                    # Best-effort only; do not fail the tagging pipeline if this step fails.
                    pass

            log.info("[NEW-TAGS]------------------")
            _print_all_tags(temp_path)
            summary["tagged"] += 1
            if had_conflict:
                log.info(
                    f"[TAGGED] {name}: completed with conflicts (some fields skipped)."
                )
            else:
                log.info(f"[TAGGED] {name}: completed with no conflicts.")

            # Rename file to Title_Artist.ext before upload
            base, ext = os.path.splitext(name)
            title_part = _safe_filename_component(updates.title)
            artist_part = _safe_filename_component(updates.artist)

            if title_part and artist_part:
                new_name = f"{title_part}_{artist_part}{ext}"
            else:
                # Fallback to original name if we cannot safely build a new one
                new_name = name

            new_temp_path = os.path.join(
                os.path.dirname(temp_path),
                f"{file_id}_{new_name}",
            )

            if new_temp_path != temp_path:
                try:
                    os.rename(temp_path, new_temp_path)
                    temp_path = new_temp_path
                    log.info(f"[RENAME] Renamed file to {new_name}")
                except Exception as e:
                    log.error(f"[RENAME-ERROR] Failed to rename {name}: {e}")

            # Upload to destination
            drive.upload_file(service, temp_path, dest_folder_id, new_name)
            summary["uploaded"] += 1
            log.info(f"[UPLOAD] {new_name} -> dest_folder_id={dest_folder_id}")

            _delete_drive_file(service, file_id)
            summary["deleted"] += 1
            log.info(f"[DELETE] Deleted source file_id={file_id} ({name})")

        except Exception as e:
            summary["failed"] += 1
            log.error(f"[ERROR] {name} ({file_id}): {e}")
        finally:
            # Best-effort cleanup
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

    log.info(f"[DONE] Summary: {summary}")
    return summary


def main() -> None:
    """
    Entrypoint.

    Expects these env vars (or you can wire it from kaiano_common_utils.config):
      - MUSIC_UPLOAD_SOURCE_FOLDER_ID
      - MUSIC_TAGGING_OUTPUT_FOLDER_ID
      - ACOUSTID_API_KEY
    """

    source_folder_id = "1hDFTDOavXDtJN-MR-ruqqapMaXGp4mB6"
    dest_folder_id = "1fL4Q4S1WUefC1QhHIsLuj3_DU1ZZBm_4"
    acoustid_api_key = "R1yQzNHear"
    if not source_folder_id or not dest_folder_id or not acoustid_api_key:
        raise RuntimeError(
            "Missing required configuration. Set env vars MUSIC_UPLOAD_SOURCE_FOLDER_ID, "
            "MUSIC_TAGGING_OUTPUT_FOLDER_ID, ACOUSTID_API_KEY (or define them in kaiano_common_utils.config)."
        )

    try:
        max_uploads_per_run = int(os.environ.get("MAX_UPLOADS_PER_RUN", "100"))
    except Exception:
        max_uploads_per_run = 100

    process_drive_folder_for_retagging(
        source_folder_id,
        dest_folder_id,
        acoustid_api_key=acoustid_api_key,
        max_uploads_per_run=max_uploads_per_run,
    )


if __name__ == "__main__":
    main()
