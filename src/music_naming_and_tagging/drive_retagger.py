from __future__ import annotations

import os
import tempfile
from typing import Any, Dict

import kaiano.logger as log
from kaiano.google import GoogleAPI
from kaiano.mp3.identify import IdentificationPolicy, Mp3Identifier
from kaiano.mp3.name import Mp3Renamer
from kaiano.mp3.tag import Mp3Tagger


def _print_all_tags(tagger: Mp3Tagger, path: str) -> None:
    printed = tagger.dump(path)
    if not printed:
        return

    log.info(f"[FILE] {os.path.basename(path)}")
    for k in sorted(printed.keys()):
        v = printed.get(k, "")
        if v is None:
            v = ""
        log.info(f"  [TAG] {k} = {v}")


def _list_music_files(g: GoogleAPI, folder_id: str) -> list[Any]:
    """List likely-audio files in a Drive folder.

    The new unified Drive facade is intentionally generic; this helper preserves the
    previous behavior of `drive.list_music_files(...)` in a local, explicit way.
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

    policy = IdentificationPolicy(
        min_confidence=min_confidence,
        max_candidates=max_candidates,
        fetch_metadata_min_confidence=min_confidence,
    )

    identifier = Mp3Identifier.from_env(
        acoustid_api_key=acoustid_api_key, policy=policy
    )
    tagger = Mp3Tagger()
    renamer = Mp3Renamer()

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
            _print_all_tags(tagger, temp_path)

            # Identify
            id_result = identifier.identify(temp_path, fetch_metadata=True)
            chosen = id_result.chosen
            chosen_conf = float(chosen.confidence) if chosen is not None else 0.0
            identified = chosen is not None and chosen_conf >= float(min_confidence)

            # Tag + rename only when we have metadata (metadata fetch is policy-gated)
            path_out = temp_path
            desired_filename = os.path.basename(temp_path)

            if id_result.metadata:
                tagger.write(path_out, id_result.metadata, ensure_virtualdj_compat=True)
                summary["tagged"] += 1

                # Rename in-place (local path only)
                rename_result = renamer.apply(path_out, metadata=id_result.metadata)
                path_out = rename_result.dest_path
                desired_filename = rename_result.dest_name

            if not identified:
                # Update-in-place scenarios: no candidates or low confidence
                if chosen is None:
                    reason = "no_candidates"
                else:
                    reason = f"low_confidence:{chosen_conf:.3f}"

                g.drive.update_file(file_id, path_out)
                summary["uploaded"] += 1
                log.info(
                    f"[UPLOAD-SOURCE] Updated in place file_id={file_id} ({name}) reason={reason}"
                )
                continue

            summary["identified"] += 1

            # Identified with sufficient confidence: upload to destination and delete original.
            g.drive.upload_file(
                path_out,
                parent_id=dest_folder_id,
                dest_name=desired_filename,
            )
            summary["uploaded"] += 1
            log.info(f"[UPLOAD] {desired_filename} -> dest_folder_id={dest_folder_id}")

            g.drive.delete_file(file_id)
            summary["deleted"] += 1
            log.info(f"[DELETE] Deleted source file_id={file_id} ({name})")

        except Exception as e:
            summary["failed"] += 1
            log.error(f"[ERROR] {name} ({file_id}): {e}")
        finally:
            # Best-effort cleanup
            try:
                paths = {temp_path}
                try:
                    if "path_out" in locals() and path_out and path_out != temp_path:
                        paths.add(path_out)
                except Exception:
                    pass

                for p in paths:
                    if p and os.path.exists(p):
                        os.remove(p)
            except Exception:
                pass

    log.info(f"[DONE] Summary: {summary}")
    return summary
