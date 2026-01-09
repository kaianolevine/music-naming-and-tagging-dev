from __future__ import annotations

import binascii
import os
import subprocess
import time
from typing import Any, Iterable, List, Optional

import acoustid
import kaiano_common_utils.logger as log
import musicbrainzngs
from retagger_types import Identifier, TagSnapshot, TrackId, TrackMetadata

# NOTE: This file contains only the "API interface" pieces:
# - AcoustID identification
# - MusicBrainz metadata fetching


class AcoustIdIdentifier(Identifier):
    """
    Uses Chromaprint fingerprinting via pyacoustid + AcoustID webservice
    to return MusicBrainz Recording MBIDs as TrackId(provider="musicbrainz").
    """

    def __init__(
        self,
        api_key: str,
        min_confidence: float = 0.90,
        max_candidates: int = 5,
        retries: int = 3,
        retry_sleep_s: float = 1.0,
    ) -> None:
        self.api_key = api_key
        self.min_confidence = min_confidence
        self.max_candidates = max_candidates
        self.retries = retries
        self.retry_sleep_s = retry_sleep_s

    def identify(self, path: str, existing: TagSnapshot) -> Iterable[TrackId]:
        basename = os.path.basename(path)

        for attempt in range(1, self.retries + 1):
            try:
                # First try: acoustid.match (does fingerprinting + lookup)
                results = acoustid.match(self.api_key, path)
                log.info(
                    f"[ACOUSTID-RAW] {basename}: match() returned {len(results)} rows"
                )

                candidates: List[TrackId] = []
                for score, recording_id, title, artist in results:
                    try:
                        score_f = float(score)
                    except Exception:
                        score_f = 0.0

                    log.info(
                        f"[ACOUSTID-RAW] {basename}: score={score_f:.3f} mbid={recording_id!r} artist={artist!r} title={title!r}"
                    )

                    if not recording_id:
                        continue
                    if score_f < self.min_confidence:
                        continue

                    candidates.append(
                        TrackId(
                            provider="musicbrainz",
                            id=str(recording_id),
                            confidence=score_f,
                        )
                    )

                # sort by confidence desc and cap
                candidates.sort(key=lambda c: c.confidence, reverse=True)
                return candidates[: self.max_candidates]

            except Exception as e:

                # Provide helpful debug for decode/fingerprint errors
                try:
                    with open(path, "rb") as fh:
                        head = fh.read(32)
                    head32_hex = binascii.hexlify(head).decode("ascii", errors="ignore")
                    size_bytes = os.path.getsize(path)
                    log.error(
                        f"[ACOUSTID-ERROR] {basename}: {e!r} size_bytes={size_bytes} head32_hex={head32_hex}"
                    )
                except Exception as _dbg_e:
                    log.error(
                        f"[ACOUSTID-ERROR] {basename}: {e!r} (dbg failed: {_dbg_e!r})"
                    )

                # Fallback: try fpcalc -> acoustid.lookup
                try:
                    p = subprocess.run(
                        ["fpcalc", "-json", path],
                        check=False,
                        capture_output=True,
                        text=True,
                    )

                    if hasattr(p, "returncode") and p.returncode == 0 and p.stdout:
                        data = p.stdout
                        duration = None
                        fingerprint = None
                        try:
                            import json as _json

                            # fpcalc should emit JSON, but some builds may prepend noise.
                            json_text = data
                            if "{" in json_text and "}" in json_text:
                                json_text = json_text[
                                    json_text.find("{") : json_text.rfind("}") + 1
                                ]

                            parsed = _json.loads(json_text)
                            duration = parsed.get("duration")
                            fingerprint = parsed.get("fingerprint")
                        except Exception as _json_e:
                            log.error(
                                f"[ACOUSTID-FALLBACK-PARSE-ERROR] {os.path.basename(path)}: {_json_e!r} sample={data[:120]!r}"
                            )
                            duration = None
                            fingerprint = None

                        if duration and fingerprint:
                            log.info(
                                f"[ACOUSTID-FALLBACK] {os.path.basename(path)}: using fpcalc fingerprint for lookup (duration={duration})"
                            )
                            try:
                                lookup = acoustid.lookup(
                                    self.api_key,
                                    fingerprint,
                                    duration,
                                    meta="recordings+releasegroups+compress",
                                )
                                results2 = (
                                    lookup.get("results", [])
                                    if isinstance(lookup, dict)
                                    else []
                                )
                                candidates2: List[TrackId] = []

                                for r in results2:
                                    score = float(r.get("score", 0.0) or 0.0)
                                    if score < self.min_confidence:
                                        continue
                                    recs = r.get("recordings") or []
                                    for rec in recs:
                                        rid = rec.get("id")
                                        if rid:
                                            candidates2.append(
                                                TrackId(
                                                    provider="musicbrainz",
                                                    id=str(rid),
                                                    confidence=score,
                                                )
                                            )

                                candidates2.sort(
                                    key=lambda c: c.confidence, reverse=True
                                )
                                return candidates2[: self.max_candidates]
                            except Exception as _lookup_e:
                                log.error(
                                    f"[ACOUSTID-FALLBACK-LOOKUP-ERROR] {basename}: {_lookup_e!r}"
                                )

                except Exception as _fpcalc_e:
                    log.error(f"[ACOUSTID-FALLBACK-ERROR] {basename}: {_fpcalc_e!r}")

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


class MusicBrainzRecordingProvider:
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
    ) -> None:
        self.throttle_s = throttle_s
        self.retries = retries
        self.retry_sleep_s = retry_sleep_s
        self._last_call_ts = 0.0

        musicbrainzngs.set_useragent(app_name, app_version, contact)

    def _throttle(self) -> None:
        # polite throttling
        delta = time.time() - self._last_call_ts
        if delta < self.throttle_s:
            time.sleep(self.throttle_s - delta)
        self._last_call_ts = time.time()

    def _best_genre(self, tags: list[dict[str, Any]] | None) -> Optional[str]:
        """
        Pick a best-effort 'genre' from a MusicBrainz tag-list.
        MusicBrainz tags are community-driven and may include non-genre concepts.
        """
        if not tags:
            return None

        # prefer highest count tags
        try:
            sorted_tags = sorted(
                tags, key=lambda t: int(t.get("count", 0) or 0), reverse=True
            )
        except Exception:
            sorted_tags = tags

        for t in sorted_tags:
            name = t.get("name")
            if name:
                return str(name)
        return None

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
                    includes=["artists", "releases", "isrcs", "tags"],
                )
                r = rec.get("recording", {})

                title = r.get("title")
                artist = None
                album = None
                isrc = None
                year = None
                genre = None

                # primary artist credit
                try:
                    ac = r.get("artist-credit") or []
                    if ac and isinstance(ac, list):
                        a0 = ac[0]
                        if isinstance(a0, dict):
                            artist = (a0.get("artist") or {}).get("name") or a0.get(
                                "name"
                            )
                except Exception:
                    artist = None

                # release / album + year
                try:
                    releases = r.get("release-list") or []
                    if releases:
                        rel0 = releases[0]
                        if isinstance(rel0, dict):
                            album = rel0.get("title")
                            date = rel0.get("date")
                            if date and isinstance(date, str) and len(date) >= 4:
                                year = date[:4]
                except Exception:
                    album = album
                    year = year

                # isrc
                try:
                    isrcs = r.get("isrc-list") or []
                    if isrcs:
                        isrc = str(isrcs[0])
                except Exception:
                    isrc = None

                # tags -> genre
                try:
                    tags = r.get("tag-list") or []
                    genre = self._best_genre(tags)
                except Exception:
                    genre = None

                return TrackMetadata(
                    title=title,
                    artist=artist,
                    album=album,
                    year=year,
                    isrc=isrc,
                    genre=genre,
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
