import os
import re
import tempfile
from typing import Dict, List, Optional

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as drive
import kaiano_common_utils.logger as log
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC
from mutagen.mp3 import MP3
from mutagen.mp4 import MP4


def new_sanitize_filename(value: str) -> str:
    # Replace any non-alphanumeric or underscore character with underscore
    value = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    return value


def sanitize_filename(value: str) -> str:
    value = re.sub(r"\s+", "_", value)
    return re.sub(r"[^a-zA-Z0-9_\-]", "", value)


# Helper to avoid filename collisions
def _unique_path(base_path: str) -> str:
    """
    Ensure the returned path does not collide with an existing file by appending
    an incrementing suffix before the extension, e.g. "name.mp3" -> "name_1.mp3".
    """
    directory, filename = os.path.dirname(base_path), os.path.basename(base_path)
    stem, ext = os.path.splitext(filename)
    candidate = base_path
    counter = 1
    while os.path.exists(candidate):
        candidate = os.path.join(directory, f"{stem}_{counter}{ext}")
        counter += 1
    return candidate


def get_metadata(file_path: str) -> Dict[str, str]:
    """
    Extract common audio metadata fields using mutagen for various formats.

    Returns keys: artist, title, bpm, comment, album, genre, year, tracknumber, key
    Missing values default to "" (empty string) except artist/title -> "Unknown".
    """
    ext = file_path.lower().split(".")[-1]
    audio = None
    if ext == "mp3":
        audio = MP3(file_path, ID3=EasyID3)
    elif ext == "flac":
        audio = FLAC(file_path)
    elif ext in ("m4a", "mp4"):
        audio = MP4(file_path)
    else:
        raise ValueError(f"Unsupported file format: {ext}")

    tags = audio.tags or {}

    def _get(tag: str, default: str = "") -> str:
        try:
            val = tags.get(tag, [default])
            if isinstance(val, (list, tuple)):
                return str(val[0]) if val else default
            return str(val)
        except Exception:
            return default

    artist = _get("artist", "Unknown")
    title = _get("title", "Unknown")
    bpm_raw = _get("bpm", "")
    try:
        bpm = str(int(round(float(bpm_raw)))) if bpm_raw not in (None, "") else ""
    except (ValueError, TypeError):
        bpm = ""

    # Common additional fields
    album = _get("album", "")
    genre = _get("genre", "")
    year = _get("date", "") or _get("year", "")
    tracknumber = _get("tracknumber", "")
    musical_key = _get("initialkey", "") or _get("key", "")
    comment = _get("comment", "")

    return {
        "artist": artist,
        "title": title,
        "bpm": bpm,
        "comment": comment,
        "album": album,
        "genre": genre,
        "year": year,
        "tracknumber": tracknumber,
        "key": musical_key,
    }


def rename_music_file(file_path: str, output_dir: str, separator: str) -> str:
    """
    Rename a single file based on extracted metadata. Returns the destination path.

    Args:
        file_path: Source file to rename.
        output_dir: Target directory to place the renamed file.
        separator: Token to join filename parts.
        extension: Optional extension override (e.g., ".mp3"). If None, preserve original.
        dry_run: If True, do not actually rename/move files; return the intended path.
    """
    metadata = get_metadata(file_path)
    filename_parts = [
        metadata.get("bpm", ""),
        metadata.get("title", ""),
        metadata.get("artist", ""),
        metadata.get("comment", ""),
    ]
    cleaned_parts = [sanitize_filename(p) for p in filename_parts if p]
    final_ext = os.path.splitext(file_path)[1]
    proposed_name = f"{separator.join(cleaned_parts)}{final_ext}"
    proposed_path = os.path.join(output_dir, proposed_name)
    dest_path = _unique_path(proposed_path)

    log.debug(f"Renaming {file_path} -> {dest_path}")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    os.rename(file_path, dest_path)
    return dest_path


def rename_files_in_directory(directory: str, config: Dict) -> Dict[str, int]:
    """
    Scan a directory recursively, renaming files according to config.

    Returns a summary dict: {"processed": int, "renamed": int, "skipped": int, "failed": int}
    """
    log.info(f"Scanning directory: {directory}")
    summary = {"processed": 0, "renamed": 0, "skipped": 0, "failed": 0}
    for root, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            if not os.path.isfile(full_path):
                continue
            summary["processed"] += 1
            try:
                metadata = get_metadata(full_path)
                new_name = generate_filename(metadata, config)
                if not new_name:
                    log.warning(f"Skipping file due to missing metadata: {file}")
                    summary["skipped"] += 1
                    continue
                new_path = os.path.join(root, new_name)
                new_path = _unique_path(new_path)
                if os.path.abspath(new_path) == os.path.abspath(full_path):
                    log.debug(f"Name unchanged for: {file}")
                    summary["skipped"] += 1
                    continue
                log.debug(f"Renaming file: {file} -> {os.path.basename(new_path)}")
                os.rename(full_path, new_path)
                summary["renamed"] += 1
                log.info(f"Renamed: {file} -> {os.path.basename(new_path)}")
            except ValueError as e:
                log.warning(f"Metadata parsing failed for {file}: {e}")
                summary["failed"] += 1
            except OSError as e:
                log.error(f"Filesystem error for {file}: {e}")
                summary["failed"] += 1
            except Exception:
                log.error(f"Failed to rename file: {file}", exc_info=True)
                summary["failed"] += 1
    log.info(f"Summary: {summary}")
    return summary


def process_drive_folder(source_folder_id, dest_folder_id, separator) -> Dict[str, int]:
    """
    Download files from Drive, rename locally, and upload back to Drive.

    Returns a summary dict: {"downloaded": int, "renamed": int, "uploaded": int, "failed": int}
    """
    log.info(
        f"Processing Drive folder with parameters: source_folder_id={source_folder_id}, dest_folder_id={dest_folder_id}, separator='{separator}'"
    )

    service = drive.get_drive_service()

    summary = {"downloaded": 0, "renamed": 0, "uploaded": 0, "failed": 0}

    music_files = drive.list_music_files(service, source_folder_id)
    for file in music_files:
        try:
            temp_path = os.path.join(tempfile.gettempdir(), file["name"])
            drive.download_file(service, file["id"], temp_path)
            summary["downloaded"] += 1
            log.debug(f"Downloaded: {file['name']} to {temp_path}")

            renamed_path = rename_music_file(
                temp_path, tempfile.gettempdir(), separator
            )
            summary["renamed"] += 1
            log.debug(f"Renamed to: {os.path.basename(renamed_path)}")

            drive.upload_file(service, renamed_path, dest_folder_id)
            summary["uploaded"] += 1
            log.debug(f"Uploaded: {os.path.basename(renamed_path)}")
        except Exception:
            log.error(
                f"Failed processing Drive file: {file.get('name')}", exc_info=True
            )
            summary["failed"] += 1
    log.info(f"Drive process summary: {summary}")
    return summary


def generate_filename(metadata: Dict[str, str], config: Dict) -> Optional[str]:
    """
    Generate a sanitized filename based on selected metadata fields and config-defined order.

    Config keys used:
      - rename_order: List[str] of field names in order
      - required_fields: List[str] of fields that must be present/non-empty
      - extension: default extension (e.g., ".mp3")
      - separator: string between parts (default "__")
    """
    log.debug(f"Generating filename using metadata: {metadata} and config: {config}")
    rename_order: List[str] = config.get("rename_order", [])
    required_fields: List[str] = config.get("required_fields", [])
    extension: str = config.get("extension", ".mp3")
    separator: str = config.get("separator", "__")

    filename_parts: List[str] = []
    for field in rename_order:
        value = metadata.get(field, "")
        log.debug(f"Field: {field}, Value: {value}")
        if not value and field in required_fields:
            log.debug(
                f"Required field '{field}' is missing, skipping filename generation."
            )
            return None
        sanitized = sanitize_filename(value)
        if sanitized:
            filename_parts.append(sanitized)

    if not filename_parts:
        log.debug("No valid fields found for filename generation, returning None.")
        return None

    filename = f"{separator.join(filename_parts)}{extension}"
    log.debug(f"Generated filename: {filename}")
    return filename


if __name__ == "__main__":
    process_drive_folder(
        config.MUSIC_UPLOAD_SOURCE_FOLDER_ID,
        config.MUSIC_TAGGING_OUTPUT_FOLDER_ID,
        config.SEP_CHARACTERS,
    )
