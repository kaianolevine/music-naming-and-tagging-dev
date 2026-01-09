from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from retagger_api import AcoustIdIdentifier, MusicBrainzRecordingProvider
from retagger_music_tag import MusicTagIO
from retagger_types import (
    Identifier,
    MetadataProvider,
    TagReaderWriter,
    TrackId,
    TrackMetadata,
)

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
    ) -> None:
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
    api_key = "R1yQzNHear"
    if not api_key:
        raise RuntimeError("Missing ACOUSTID_API_KEY environment variable")

    tag_io = MusicTagIO()
    identifier = AcoustIdIdentifier(
        api_key=api_key, min_confidence=0.90, max_candidates=5
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
