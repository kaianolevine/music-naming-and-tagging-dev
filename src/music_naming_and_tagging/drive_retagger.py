from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, Tuple

import kaiano_common_utils.helpers as helpers
import kaiano_common_utils.logger as log
from kaiano_common_utils.api.google import GoogleAPI
from kaiano_common_utils.api.identify_audio import IdentificationPolicy, IdentifyAudio
from kaiano_common_utils.api.music_tag.retagger_types import TagSnapshot, TrackMetadata


def _print_all_tags(ia: IdentifyAudio, path: str) -> None:
    printed = ia.tags.dump(path)
    if not printed:
        return

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
    """Build updates while avoiding overwriting conflicting existing tags.

    Option B rules:
    - Only write a new value if:
      * the new value is non-empty AND
      * the existing value is empty OR normalized(existing) == normalized(new)
    - If existing and new differ (after normalization), skip overwrite and mark conflict.

    Special cases:
    - Genre is fill-only (only write if existing genre is empty and new genre present).
    - Comment is always written using the <KAT_v1> prefix rules.
    """

    t = existing.tags

    def _blank(v: Any) -> bool:
        return helpers.safe_str(v).strip() == ""

    def _norm(v: Any) -> str:
        return helpers.normalize_for_compare(helpers.safe_str(v).strip())

    def _pick(
        existing_val: Any, new_val: Any, *, normalize_fn
    ) -> Tuple[Any | None, bool]:
        """Return (value_to_write_or_None, conflict_bool)."""
        if _blank(new_val):
            return None, False

        if _blank(existing_val):
            return new_val, False

        if normalize_fn(existing_val) == normalize_fn(new_val):
            # Safe to write (can normalize casing/format)
            return new_val, False

        # Conflict: do not overwrite
        return None, True

    had_conflict = False

    existing_title = t.get("tracktitle")
    existing_artist = t.get("artist")
    existing_album = t.get("album")
    existing_album_artist = t.get("albumartist")
    existing_year_raw = t.get("year") or t.get("date")
    existing_bpm = t.get("bpm")
    existing_isrc = t.get("isrc")
    existing_track_number = t.get("tracknumber")
    existing_disc_number = t.get("discnumber")

    # Normalize year values before compare so formatting differences don't create conflicts
    new_year_norm = helpers.normalize_year_for_tag(new_meta.year)
    existing_year_norm = helpers.normalize_year_for_tag(existing_year_raw)

    def _norm_year(v: Any) -> str:
        return helpers.normalize_for_compare(
            helpers.safe_str(helpers.normalize_year_for_tag(v)).strip()
        )

    # BPM compare uses normalize_for_compare so formats like "87" and "87.0" don't conflict
    def _norm_bpm(v: Any) -> str:
        return _norm(v)

    title_to_write, c = _pick(existing_title, new_meta.title, normalize_fn=_norm)
    had_conflict = had_conflict or c

    artist_to_write, c = _pick(existing_artist, new_meta.artist, normalize_fn=_norm)
    had_conflict = had_conflict or c

    album_to_write, c = _pick(existing_album, new_meta.album, normalize_fn=_norm)
    had_conflict = had_conflict or c

    album_artist_to_write, c = _pick(
        existing_album_artist, new_meta.album_artist, normalize_fn=_norm
    )
    had_conflict = had_conflict or c

    year_to_write, c = _pick(existing_year_norm, new_year_norm, normalize_fn=_norm_year)
    had_conflict = had_conflict or c

    bpm_to_write, c = _pick(existing_bpm, new_meta.bpm, normalize_fn=_norm_bpm)
    had_conflict = had_conflict or c

    isrc_to_write, c = _pick(existing_isrc, new_meta.isrc, normalize_fn=_norm)
    had_conflict = had_conflict or c

    track_number_to_write, c = _pick(
        existing_track_number, new_meta.track_number, normalize_fn=_norm
    )
    had_conflict = had_conflict or c

    disc_number_to_write, c = _pick(
        existing_disc_number, new_meta.disc_number, normalize_fn=_norm
    )
    had_conflict = had_conflict or c

    # Genre fill-only
    existing_genre = helpers.normalize_for_compare(t.get("genre"))
    new_genre = helpers.normalize_for_compare(new_meta.genre)
    genre_to_write = new_meta.genre if (not existing_genre and new_genre) else None

    # Comment prefixing (always written)
    existing_comment = helpers.safe_str(t.get("comment")).strip()
    if existing_comment == "":
        comment_to_write = "<KAT_v1>"
    elif existing_comment.startswith("<KAT_v1>"):
        comment_to_write = existing_comment
    else:
        comment_to_write = "<KAT_v1> " + existing_comment

    updates = TrackMetadata(
        # Null-safe casing: only title-case if value present
        title=helpers.title_case_words(title_to_write) if title_to_write else None,
        artist=helpers.title_case_words(artist_to_write) if artist_to_write else None,
        album=album_to_write if album_to_write else None,
        album_artist=album_artist_to_write if album_artist_to_write else None,
        year=year_to_write if year_to_write else None,
        # Genre is fill-only. If we are not writing genre, keep it as None so we do not
        # overwrite an existing genre tag with an empty string.
        genre=(
            helpers.title_case_words(genre_to_write)
            if genre_to_write is not None
            else None
        ),
        bpm=bpm_to_write if bpm_to_write else None,
        comment=comment_to_write,
        isrc=isrc_to_write if isrc_to_write else None,
        track_number=track_number_to_write if track_number_to_write else None,
        disc_number=disc_number_to_write if disc_number_to_write else None,
        raw=new_meta.raw,
    )

    return updates, had_conflict


def _build_passthrough_updates_from_snapshot(snapshot: TagSnapshot) -> TrackMetadata:
    """Build updates from tags we already read, to re-write them in a VDJ-friendly way.

    Purpose: sometimes files have tags that `music_tag` can read (and we can log), but
    VirtualDJ doesn't show them reliably (often due to ID3 version/encoding quirks).

    This function constructs a TrackMetadata using the currently-read tags and lets
    `MusicTagIO.write()` normalize/write them back out. We keep this conservative and
    do not invent missing values.
    """
    t = snapshot.tags

    title = helpers.safe_str(t.get("tracktitle")).strip()
    artist = helpers.safe_str(t.get("artist")).strip()
    album = helpers.safe_str(t.get("album")).strip()
    album_artist = helpers.safe_str(t.get("albumartist")).strip()
    genre = helpers.safe_str(t.get("genre")).strip()
    bpm = helpers.safe_str(t.get("bpm")).strip()
    comment = helpers.safe_str(t.get("comment")).strip()
    year = helpers.normalize_year_for_tag(t.get("year") or t.get("date"))

    track_number = helpers.safe_str(t.get("tracknumber")).strip()
    disc_number = helpers.safe_str(t.get("discnumber")).strip()

    return TrackMetadata(
        title=title if title else None,
        artist=artist if artist else None,
        album=album if album else None,
        album_artist=album_artist if album_artist else None,
        year=year if year else None,
        genre=genre if genre else None,
        bpm=bpm if bpm else None,
        comment=comment if comment else None,
        isrc=None,
        track_number=track_number if track_number else None,
        disc_number=disc_number if disc_number else None,
        raw=getattr(snapshot, "raw", None),
    )


def _list_music_files(g: GoogleAPI, folder_id: str) -> list[Any]:
    """List likely-audio files in a Drive folder.

    The new unified Drive facade is intentionally generic; this helper preserves the
    previous behavior of `drive.list_music_files(...)` in a local, explicit way.

    Returns DriveFile objects with at least `id` and `name` attributes.
    """

    # Common audio MIME types encountered in Drive.
    mime_types = [
        "audio/mpeg",  # mp3
        "audio/mp4",  # m4a/mp4 audio
        "audio/x-m4a",  # sometimes used for m4a
        "audio/wav",
        "audio/x-wav",
        "audio/flac",
        "audio/aac",
        "audio/ogg",
        "audio/x-aiff",
        "audio/aiff",
    ]

    files: list[Any] = []
    seen: set[str] = set()

    for mt in mime_types:
        for f in g.drive.list_files(parent_id=folder_id, mime_type=mt, trashed=False):
            fid = getattr(f, "id", None)
            if not fid or fid in seen:
                continue
            seen.add(fid)
            files.append(f)

    # Fallback: if nothing matched by mime type, return everything in the folder.
    # This mirrors prior behavior where Drive metadata was occasionally inconsistent.
    if not files:
        files = g.drive.list_files(parent_id=folder_id, trashed=False)

    return files


def process_drive_folder_for_retagging(
    source_folder_id: str,
    dest_folder_id: str,
    *,
    acoustid_api_key: str,
    min_confidence: float = 0.90,
    max_candidates: int = 5,
    max_uploads_per_run: int = 200,
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
    g = GoogleAPI.from_env()

    ia = IdentifyAudio.from_env(
        acoustid_api_key=acoustid_api_key,
        id_policy=IdentificationPolicy(
            min_confidence=min_confidence,
            max_candidates=max_candidates,
        ),
        app_name="music-naming-and-tagging",
        app_version="0.1.0",
        contact="https://example.com",
        throttle_s=1.0,
    )

    summary = {
        "scanned": 0,
        "downloaded": 0,
        "identified": 0,
        "tagged": 0,
        "uploaded": 0,
        "failed": 0,
        "deleted": 0,
    }

    music_files = _list_music_files(g, source_folder_id)
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
        file_id = getattr(file, "id", None)
        name = getattr(file, "name", "unknown")

        if not file_id:
            log.info(f"[SKIP] Missing file id for {name!r}; skipping.")
            continue

        temp_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{name}")

        try:
            log.info(f"[DOWNLOAD] {name} ({file_id}) -> {temp_path}")
            g.drive.download_file(file_id, temp_path)
            summary["downloaded"] += 1

            # Print existing tags
            log.info("[PRE-EXISTING-TAGS]------------------")
            _print_all_tags(ia, temp_path)

            # Identify
            snapshot = ia.tags.read(temp_path)
            candidates = ia.identify.candidates(temp_path, snapshot)
            _log_candidate_options(name, candidates, max_show=max_candidates)

            if not candidates:
                log.info(
                    f"[IDENTIFY-SKIP] {name}: no candidates returned (re-writing existing tags + re-uploading to source for VirtualDJ compatibility)"
                )

                # Even if we can't identify the track, re-write the tags we can already read.
                # This frequently fixes VirtualDJ showing only title.
                passthrough_updates = _build_passthrough_updates_from_snapshot(snapshot)
                ia.tags.write(
                    temp_path,
                    passthrough_updates,
                    ensure_virtualdj_compat=True,
                )

                # Upload back to the SAME source folder, replacing the original file.
                g.drive.upload_file(
                    temp_path, parent_id=source_folder_id, dest_name=name
                )
                summary["uploaded"] += 1
                log.info(
                    f"[UPLOAD-SOURCE] {name} -> source_folder_id={source_folder_id}"
                )

                g.drive.delete_file(file_id)
                summary["deleted"] += 1
                log.info(f"[DELETE] Deleted source file_id={file_id} ({name})")

                # Count as tagged because we performed a tag write.
                summary["tagged"] += 1
                continue

            chosen = max(candidates, key=lambda c: c.confidence)
            if chosen.confidence < min_confidence:
                log.info(
                    f"[IDENTIFY-SKIP] {name}: best score {chosen.confidence:.3f} below threshold {min_confidence:.2f} (re-writing existing tags + re-uploading to source for VirtualDJ compatibility)"
                )

                passthrough_updates = _build_passthrough_updates_from_snapshot(snapshot)
                ia.tags.write(
                    temp_path,
                    passthrough_updates,
                    ensure_virtualdj_compat=True,
                )

                g.drive.upload_file(
                    temp_path, parent_id=source_folder_id, dest_name=name
                )
                summary["uploaded"] += 1
                log.info(
                    f"[UPLOAD-SOURCE] {name} -> source_folder_id={source_folder_id}"
                )

                g.drive.delete_file(file_id)
                summary["deleted"] += 1
                log.info(f"[DELETE] Deleted source file_id={file_id} ({name})")

                summary["tagged"] += 1
                continue

            summary["identified"] += 1
            log.info(
                f"[IDENTIFY] {name}: provider={chosen.provider} id={chosen.id} score={chosen.confidence:.3f}"
            )

            # Fetch metadata
            meta = ia.metadata.fetch(chosen)
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
            ia.tags.write(
                temp_path,
                updates,
                ensure_virtualdj_compat=True,
            )

            log.info("[NEW-TAGS]------------------")
            _print_all_tags(ia, temp_path)
            summary["tagged"] += 1
            if had_conflict:
                log.info(
                    f"[TAGGED] {name}: completed with conflicts (some fields skipped)."
                )
            else:
                log.info(f"[TAGGED] {name}: completed with no conflicts.")

            # Rename file to Title_Artist.ext before upload
            base, ext = os.path.splitext(name)
            title_part = helpers.safe_filename_component(updates.title)
            artist_part = helpers.safe_filename_component(updates.artist)

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
            g.drive.upload_file(temp_path, parent_id=dest_folder_id, dest_name=new_name)
            summary["uploaded"] += 1
            log.info(f"[UPLOAD] {new_name} -> dest_folder_id={dest_folder_id}")

            g.drive.delete_file(file_id)
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
