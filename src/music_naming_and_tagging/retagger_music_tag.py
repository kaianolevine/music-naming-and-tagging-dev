from __future__ import annotations

import os
from typing import Dict, Optional

import kaiano_common_utils.logger as log
import music_tag
from mutagen.id3 import ID3, TDRC, TYER, ID3NoHeaderError

from music_naming_and_tagging.retagger_types import (
    TagReaderWriter,
    TagSnapshot,
    TrackMetadata,
)

# ---------- music-tag adapter ----------
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


class MusicTagIO(TagReaderWriter):
    def _normalize_year_for_tag(self, v: Optional[str]) -> str:
        s = "" if v is None else str(v).strip()
        if not s:
            return ""
        if len(s) >= 4 and s[:4].isdigit():
            return s[:4]
        return ""

    def _save_virtualdj_id3_compat(self, path: str, year: Optional[str]) -> None:
        """Best-effort: ensure VirtualDJ-friendly ID3v2.3 save (mp3 only).

        Historically, VirtualDJ is more reliable with ID3v2.3 and TYER.
        - If `year` is provided, we also set TYER and TDRC.
        - Always saves as ID3v2.3.

        This is best-effort and should never raise.
        """
        try:
            ext = os.path.splitext(path)[1].lower().lstrip(".")
            if ext != "mp3":
                return

            try:
                id3 = ID3(path)
            except ID3NoHeaderError:
                id3 = ID3()

            normalized_year = self._normalize_year_for_tag(year)
            if normalized_year:
                try:
                    id3.setall("TYER", [TYER(encoding=3, text=normalized_year)])
                except Exception:
                    pass
                try:
                    id3.setall("TDRC", [TDRC(encoding=3, text=normalized_year)])
                except Exception:
                    pass

            id3.save(path, v2_version=3)
        except Exception:
            # best-effort only
            return

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

    def write(
        self,
        path: str,
        updates: TrackMetadata,
        *,
        ensure_virtualdj_compat: bool = False,
    ) -> None:
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
        if ensure_virtualdj_compat:
            self._save_virtualdj_id3_compat(path, updates.year)

    def dump_tags(self, path: str) -> Dict[str, str]:
        """Return a stable dict of tags for logging/debug.

        - Includes a curated set of common keys (TAG_FIELDS)
        - Attempts to include any additional keys exposed by the backend
        - Never raises; best-effort
        """
        try:
            f = music_tag.load_file(path)
        except Exception as e:
            log.error(
                f"[TAGS-ERROR] Failed to read tags for {os.path.basename(path)}: {e}"
            )
            return {}

        printed: Dict[str, str] = {}

        # Curated keys
        for k in TAG_FIELDS:
            try:
                if k == "artwork":
                    continue
                v = f[k]
                if isinstance(v, list):
                    printed[k] = ", ".join([str(x) for x in v if x is not None])
                else:
                    printed[k] = "" if v is None else str(v)
            except Exception:
                printed[k] = ""

        # Extra keys (best-effort)
        extra_keys = []
        try:
            extra_keys = [
                k for k in getattr(f, "keys")() if k not in printed and k != "artwork"
            ]
        except Exception:
            extra_keys = []

        for k in sorted(extra_keys):
            try:
                v = f[k]
                if isinstance(v, list):
                    printed[k] = ", ".join([str(x) for x in v if x is not None])
                else:
                    printed[k] = "" if v is None else str(v)
            except Exception:
                continue

        return printed
