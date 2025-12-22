from __future__ import annotations

import binascii
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Protocol

import acoustid

# import kaiano_common_utils.config as config
import kaiano_common_utils.logger as log
import music_tag
import musicbrainzngs

# ---------- Models / Interfaces ----------


@dataclass(frozen=True)
class TrackId:
    provider: str
    id: str
    confidence: float = 1.0


@dataclass
class TrackMetadata:
    title: Optional[str] = None
    artist: Optional[str] = None
    album: Optional[str] = None
    album_artist: Optional[str] = None
    year: Optional[str] = None
    genre: Optional[str] = None
    bpm: Optional[str] = None
    comment: Optional[str] = None
    isrc: Optional[str] = None
    track_number: Optional[str] = None
    disc_number: Optional[str] = None
    raw: Dict[str, Any] = None  # set in init

    def __post_init__(self) -> None:
        if self.raw is None:
            self.raw = {}


@dataclass(frozen=True)
class TagSnapshot:
    tags: Dict[str, Any]
    has_artwork: bool = False


class TagReaderWriter(Protocol):
    def read(self, path: str) -> TagSnapshot: ...
    def write(self, path: str, updates: TrackMetadata) -> None: ...


class Identifier(Protocol):
    def identify(self, path: str, existing: TagSnapshot) -> Iterable[TrackId]: ...


class MetadataProvider(Protocol):
    def fetch(self, track_id: TrackId) -> TrackMetadata: ...


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
            "genre",
            "comment",
            "isrc",
            "tracknumber",
            "discnumber",
            "bpm",
            "artwork",
        ]
        tags: dict[str, Any] = {}
        has_artwork = False

        for k in keys:
            try:
                v = f[k]
                if k == "artwork":
                    if v:
                        has_artwork = True
                    continue
                if v is not None and str(v).strip() != "":
                    tags[k] = v
            except KeyError:
                continue
            except Exception:
                continue

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

        for k, v in mapping.items():
            if v is None:
                continue
            # NOTE: overwrite/fill-only policy intentionally deferred.
            f[k] = str(v)

        f.save()


# ---------- AcoustID Identifier ----------


class AcoustIdIdentifier(Identifier):
    """
    Uses Chromaprint fingerprinting via pyacoustid + AcoustID webservice
    to return MusicBrainz Recording MBIDs as TrackId(provider="musicbrainz").
    """

    def __init__(
        self,
        api_key: str,
        min_confidence: float = 0.70,
        max_candidates: int = 5,
        retries: int = 3,
        retry_sleep_s: float = 1.5,
    ):
        self.api_key = api_key
        self.min_confidence = min_confidence
        self.max_candidates = max_candidates
        self.retries = retries
        self.retry_sleep_s = retry_sleep_s

    def identify(self, path: str, existing: TagSnapshot) -> Iterable[TrackId]:

        for attempt in range(1, self.retries + 1):
            try:
                # returns list of tuples: (score, recording_id, title, artist)
                results = acoustid.match(self.api_key, path)
                basename = os.path.basename(path)
                log.info(
                    f"[ACOUSTID-RAW] {basename}: match() returned {len(results)} rows"
                )
                for score, recording_id, title, artist in sorted(
                    results, key=lambda r: r[0], reverse=True
                )[:5]:
                    log.info(
                        f"[ACOUSTID-RAW] {basename}: score={float(score):.3f} mbid={recording_id!r} artist={artist!r} title={title!r}"
                    )

                if not results:
                    return []

                # Sort by score desc, keep only unique recording IDs
                seen = set()
                ranked: List[TrackId] = []

                for score, recording_id, _title, _artist in sorted(
                    results, key=lambda r: r[0], reverse=True
                ):
                    if not recording_id or recording_id in seen:
                        continue
                    seen.add(recording_id)

                    ranked.append(
                        TrackId(
                            provider="musicbrainz",
                            id=recording_id,
                            confidence=float(score),
                        )
                    )

                    if len(ranked) >= self.max_candidates:
                        break

                return ranked

            except acoustid.FingerprintGenerationError as e:
                try:
                    size_bytes = os.path.getsize(path)
                except Exception:
                    size_bytes = -1

                try:
                    with open(path, "rb") as fh:
                        head = fh.read(32)
                    head_hex = binascii.hexlify(head).decode("ascii")
                except Exception:
                    head_hex = "<unreadable>"

                log.error(
                    f"[ACOUSTID-DECODE-ERROR] {os.path.basename(path)}: {e!r} size_bytes={size_bytes} head32_hex={head_hex}"
                )
                # Retrying won't help if the audio can't be decoded.
                return []

            except Exception as e:
                log.error(
                    f"[ACOUSTID-ERROR] {os.path.basename(path)} attempt {attempt}/{self.retries}: {e!r}"
                )
                if attempt < self.retries:
                    time.sleep(self.retry_sleep_s * attempt)
                else:
                    # Give up: return nothing (driver can log/handle)
                    return []

        # Unreachable, but keeps type-checkers happy
        return []


# ---------- MusicBrainz Provider ----------


class MusicBrainzRecordingProvider(MetadataProvider):
    """
    Fetches metadata for a MusicBrainz recording MBID.
    """

    def __init__(
        self,
        app_name: str = "music-tagger",
        app_version: str = "0.1.0",
        contact: str = "https://example.com",
        throttle_s: float = 1.0,
        retries: int = 3,
        retry_sleep_s: float = 1.0,
    ):
        musicbrainzngs.set_useragent(app_name, app_version, contact)
        self.throttle_s = throttle_s
        self.retries = retries
        self.retry_sleep_s = retry_sleep_s
        self._last_call_ts = 0.0

    def _throttle(self) -> None:
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.throttle_s:
            time.sleep(self.throttle_s - elapsed)
        self._last_call_ts = time.time()

    def fetch(self, track_id: TrackId) -> TrackMetadata:
        if track_id.provider != "musicbrainz":
            raise ValueError(
                f"MusicBrainzRecordingProvider only supports provider='musicbrainz', got {track_id.provider}"
            )

        last_err: Optional[Exception] = None

        for attempt in range(1, self.retries + 1):
            try:
                self._throttle()
                rec = musicbrainzngs.get_recording_by_id(
                    track_id.id,
                    includes=["artists", "releases", "isrcs"],
                )
                r = rec.get("recording", {})

                title = r.get("title")
                artist = None
                album = None
                isrc = None
                year = None

                # Artist
                ac = r.get("artist-credit")
                if ac and isinstance(ac, list) and "artist" in ac[0]:
                    artist = ac[0]["artist"].get("name")

                # Release / Album (pick first release if present)
                releases = r.get("release-list") or r.get("releases")
                if releases and isinstance(releases, list):
                    first = releases[0]
                    album = first.get("title")
                    # Some releases include date like "2019-03-22"
                    date = first.get("date") or first.get("first-release-date")
                    if date and isinstance(date, str) and len(date) >= 4:
                        year = date[:4]

                # ISRC
                isrc_list = r.get("isrc-list")
                if isrc_list and isinstance(isrc_list, list):
                    isrc = isrc_list[0]

                return TrackMetadata(
                    title=title,
                    artist=artist,
                    album=album,
                    year=year,
                    isrc=isrc,
                    raw={"musicbrainz_recording": r},
                )

            except Exception as e:
                last_err = e
                if attempt < self.retries:
                    time.sleep(self.retry_sleep_s * attempt)
                else:
                    raise RuntimeError(
                        f"MusicBrainz fetch failed for {track_id.id}: {last_err}"
                    ) from last_err

        raise RuntimeError(f"MusicBrainz fetch failed for {track_id.id}: {last_err}")


# ---------- Driver wiring (no artwork) ----------


@dataclass
class TaggingResult:
    path: str
    identified: list[TrackId]
    chosen: Optional[TrackId]
    metadata: Optional[TrackMetadata]
    wrote_tags: bool


class TaggingDriver:
    def __init__(
        self,
        tag_io: TagReaderWriter,
        identifier: Identifier,
        meta_provider: MetadataProvider,
    ):
        self.tag_io = tag_io
        self.identifier = identifier
        self.meta_provider = meta_provider

    def run(self, path: str) -> TaggingResult:
        snapshot = self.tag_io.read(path)
        candidates = list(self.identifier.identify(path, snapshot))
        chosen = max(candidates, key=lambda c: c.confidence, default=None)

        if not chosen:
            return TaggingResult(path, candidates, None, None, False)

        meta = self.meta_provider.fetch(chosen)
        self.tag_io.write(path, meta)
        return TaggingResult(path, candidates, chosen, meta, True)


def build_driver_acoustid_musicbrainz() -> TaggingDriver:
    # api_key = os.environ.get("ACOUSTID_API_KEY") or getattr(config, "ACOUSTID_API_KEY", None)
    api_key = "qjhrUALpPV"
    if not api_key:
        raise RuntimeError("Missing ACOUSTID_API_KEY environment variable")

    tag_io = MusicTagIO()
    identifier = AcoustIdIdentifier(
        api_key=api_key, min_confidence=0.30, max_candidates=5
    )
    meta_provider = MusicBrainzRecordingProvider(
        app_name="music-tagger",
        app_version="0.1.0",
        contact="https://example.com",
        throttle_s=1.0,
    )
    return TaggingDriver(
        tag_io=tag_io, identifier=identifier, meta_provider=meta_provider
    )
