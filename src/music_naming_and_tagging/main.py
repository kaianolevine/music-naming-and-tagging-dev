from __future__ import annotations

from music_naming_and_tagging.drive_retagger import process_drive_folder_for_retagging


def main() -> None:
    """
    Entrypoint.

    Expects these env vars (or you can wire it from kaiano.config):
      - MUSIC_UPLOAD_SOURCE_FOLDER_ID
      - MUSIC_TAGGING_OUTPUT_FOLDER_ID
      - ACOUSTID_API_KEY
      - MAX_UPLOADS_PER_RUN
    """

    source_folder_id = "1hDFTDOavXDtJN-MR-ruqqapMaXGp4mB6"
    dest_folder_id = "1fL4Q4S1WUefC1QhHIsLuj3_DU1ZZBm_4"
    acoustid_api_key = "R1yQzNHear"
    if not source_folder_id or not dest_folder_id or not acoustid_api_key:
        raise RuntimeError(
            "Missing required configuration. Set env vars MUSIC_UPLOAD_SOURCE_FOLDER_ID, "
            "MUSIC_TAGGING_OUTPUT_FOLDER_ID, ACOUSTID_API_KEY (or define them in kaiano.config)."
        )

    max_uploads_per_run = 200
    process_drive_folder_for_retagging(
        source_folder_id,
        dest_folder_id,
        acoustid_api_key=acoustid_api_key,
        max_uploads_per_run=max_uploads_per_run,
    )


if __name__ == "__main__":
    main()
