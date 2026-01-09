from __future__ import annotations

import kaiano_common_utils.logger as log
import music_tag

from music_naming_and_tagging.retagger_types import (
    TagReaderWriter,
    TagSnapshot,
    TrackMetadata,
)

# ---------- music-tag adapter ----------


class MusicTagIO(TagReaderWriter):
    def read(self, path: str) -> TagSnapshot:
        f = music_tag.load_file(path)
        keys = [
            "tracktitle",
            "artist",
            "album",
            "albumartist",
            "year",
            "date",
            "genre",
            "comment",
            "isrc",
            "tracknumber",
            "discnumber",
            "bpm",
        ]

        tags = {}
        for k in keys:
            try:
                if k in f:
                    v = f[k]
                    # music_tag values can be lists/frames; coerce to a simple string-ish repr
                    if isinstance(v, list):
                        tags[k] = ", ".join([str(x) for x in v if x is not None])
                    else:
                        tags[k] = str(v)
            except Exception as e:
                log.error(f"[TAG-READ] {path}: failed reading {k}: {e!r}")

        has_artwork = False
        try:
            has_artwork = "artwork" in f and bool(f["artwork"])
        except Exception:
            has_artwork = False

        return TagSnapshot(tags=tags, has_artwork=has_artwork)

    def write(self, path: str, updates: TrackMetadata) -> None:
        f = music_tag.load_file(path)

        mapping = {
            "tracktitle": updates.title,
            "artist": updates.artist,
            "album": updates.album,
            "albumartist": updates.album_artist,
            "year": updates.year,
            "genre": updates.genre,
            "comment": updates.comment,
            "isrc": updates.isrc,
            "tracknumber": updates.track_number,
            "discnumber": updates.disc_number,
            "bpm": updates.bpm,
        }

        for key, val in mapping.items():
            if val is None:
                continue
            try:
                f[key] = str(val)
            except Exception as e:
                log.error(f"[TAG-WRITE] {path}: failed setting {key}={val!r}: {e!r}")

        f.save()
