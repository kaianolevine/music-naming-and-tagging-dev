from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Protocol

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
    comment: Optional[str] = None
    isrc: Optional[str] = None
    track_number: Optional[str] = None
    disc_number: Optional[str] = None
    bpm: Optional[str] = None
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
