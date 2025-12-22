from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, Tuple

# import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as drive
import kaiano_common_utils.logger as log
import music_tag

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


def _normalize_for_compare(v: Any) -> str:
    """Canonical comparison: None / 'None' / whitespace all become empty string."""
    return _safe_str(v).strip()


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
    Always overwrite existing tag values with new metadata when provided.

    Returns:
      (updates, had_conflict=False)
    """
    updates = TrackMetadata(
        title=new_meta.title,
        artist=new_meta.artist,
        album=new_meta.album,
        album_artist=new_meta.album_artist,
        year=new_meta.year,
        genre=new_meta.genre,
        bpm=new_meta.bpm,
        comment=new_meta.comment,
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
    min_confidence: float = 0.70,
    max_candidates: int = 5,
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
    }

    music_files = drive.list_music_files(service, source_folder_id)
    log.info(f"[START] Found {len(music_files)} music files in source folder.")

    for file in music_files:
        summary["scanned"] += 1
        file_id = file.get("id")
        name = file.get("name", "unknown")
        temp_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{name}")

        try:
            log.info(f"[DOWNLOAD] {name} ({file_id}) -> {temp_path}")
            drive.download_file(service, file_id, temp_path)
            summary["downloaded"] += 1

            # Print existing tags
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
            summary["tagged"] += 1
            if had_conflict:
                log.info(
                    f"[TAGGED] {name}: completed with conflicts (some fields skipped)."
                )
            else:
                log.info(f"[TAGGED] {name}: completed with no conflicts.")

            # Upload to destination
            drive.upload_file(service, temp_path, dest_folder_id)
            summary["uploaded"] += 1
            log.info(f"[UPLOAD] {name} -> dest_folder_id={dest_folder_id}")

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

    source_folder_id = "1Iu5TwzOXVqCDef2X8S5TZcFo1NdSHpRU"
    # os.environ.get("MUSIC_UPLOAD_SOURCE_FOLDER_ID") or getattr(
    #    config, "1Iu5TwzOXVqCDef2X8S5TZcFo1NdSHpRU", None
    # )
    dest_folder_id = "17LjjgX4bFwxR4NOnnT38Aflp8DSPpjOu"
    # os.environ.get("MUSIC_TAGGING_OUTPUT_FOLDER_ID") or getattr(
    # config, "17LjjgX4bFwxR4NOnnT38Aflp8DSPpjOu", None
    # )
    acoustid_api_key = "R1yQzNHear"
    # os.environ.get("ACOUSTID_API_KEY") or getattr(
    #    config, "qjhrUALpPV", None
    # )

    if not source_folder_id or not dest_folder_id or not acoustid_api_key:
        raise RuntimeError(
            "Missing required configuration. Set env vars MUSIC_UPLOAD_SOURCE_FOLDER_ID, "
            "MUSIC_TAGGING_OUTPUT_FOLDER_ID, ACOUSTID_API_KEY (or define them in kaiano_common_utils.config)."
        )

    process_drive_folder_for_retagging(
        source_folder_id,
        dest_folder_id,
        acoustid_api_key=acoustid_api_key,
    )


if __name__ == "__main__":
    main()
