import os

import mutagen
import pytest
from kaiano_common_utils import renamer

# =====================================================
# Helpers
# =====================================================


class DummyAudio:
    def __init__(self, tags=None):
        self.tags = tags or {}


# =====================================================
# new_sanitize_filename / sanitize_filename
# =====================================================


def test_new_sanitize_filename_replaces_special_chars():
    assert renamer.new_sanitize_filename("A!B@C") == "A_B_C"


def test_sanitize_filename_replaces_whitespace_and_non_alphanumerics():
    assert renamer.sanitize_filename("My Song (Remix)!") == "My_Song_Remix"


# =====================================================
# _unique_path
# =====================================================


def test_unique_path_adds_suffix(tmp_path):
    base = tmp_path / "test.mp3"
    base.write_text("x")
    result = renamer._unique_path(str(base))
    assert result.endswith("_1.mp3")
    assert result != str(base)


# =====================================================
# get_metadata
# =====================================================


def test_get_metadata_file_not_found_raises_mutagen():
    with pytest.raises(mutagen.MutagenError):
        renamer.get_metadata("/tmp/no_file.mp3")


def test_get_metadata_invalid_extension(tmp_path):
    f = tmp_path / "bad.wav"
    f.write_text("x")
    with pytest.raises(ValueError):
        renamer.get_metadata(str(f))


# =====================================================
# rename_music_file
# =====================================================


def test_rename_music_file_creates_dir(monkeypatch, tmp_path):
    src = tmp_path / "a.mp3"
    src.write_text("x")

    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.get_metadata",
        lambda f: {"bpm": "120", "title": "Song", "artist": "Artist", "comment": ""},
    )

    output = tmp_path / "out"
    dest = renamer.rename_music_file(str(src), str(output), "__")
    assert os.path.exists(dest)
    assert dest.endswith(".mp3")


def test_rename_music_file_duplicate(monkeypatch, tmp_path):
    src = tmp_path / "track.mp3"
    src.write_text("x")

    dest_dir = tmp_path / "target"
    dest_dir.mkdir()
    (dest_dir / "120__Track__Artist.mp3").write_text("x")

    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.get_metadata",
        lambda f: {"bpm": "120", "title": "Track", "artist": "Artist", "comment": ""},
    )
    dest = renamer.rename_music_file(str(src), str(dest_dir), "__")
    assert "_1.mp3" in dest


def test_rename_music_file_permission_error(monkeypatch, tmp_path):
    src = tmp_path / "song.mp3"
    src.write_text("x")
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.get_metadata",
        lambda f: {"bpm": "100", "title": "Song", "artist": "Artist", "comment": ""},
    )
    # simulate rename no-op
    monkeypatch.setattr("os.rename", lambda s, d: None)
    dest = renamer.rename_music_file(str(src), str(tmp_path), "__")
    assert dest.endswith(".mp3")


# =====================================================
# generate_filename
# =====================================================


def test_generate_filename_success():
    meta = {"artist": "A", "title": "B"}
    cfg = {"rename_order": ["artist", "title"], "separator": "__", "extension": ".mp3"}
    assert renamer.generate_filename(meta, cfg) == "A__B.mp3"


def test_generate_filename_missing_required_field_returns_none():
    meta = {"title": "X"}
    cfg = {"rename_order": ["artist", "title"], "required_fields": ["artist"]}
    assert renamer.generate_filename(meta, cfg) is None


def test_generate_filename_empty_metadata_returns_none():
    meta = {}
    cfg = {"rename_order": ["artist", "title"]}
    assert renamer.generate_filename(meta, cfg) is None


# =====================================================
# rename_files_in_directory
# =====================================================


def test_rename_files_in_directory_full_flow(monkeypatch, tmp_path):
    f = tmp_path / "file.mp3"
    f.write_text("x")

    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.get_metadata",
        lambda _: {"artist": "A", "title": "B"},
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.generate_filename", lambda m, c: "A__B.mp3"
    )
    monkeypatch.setattr("os.rename", lambda s, d: None)

    res = renamer.rename_files_in_directory(
        str(tmp_path), {"rename_order": ["artist", "title"]}
    )
    assert res["processed"] == 1
    assert res["renamed"] >= 0


def test_rename_files_in_directory_handles_exceptions(monkeypatch, tmp_path):
    f = tmp_path / "bad.mp3"
    f.write_text("x")
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.get_metadata",
        lambda _: (_ for _ in ()).throw(ValueError("fail")),
    )
    res = renamer.rename_files_in_directory(str(tmp_path), {"rename_order": ["title"]})
    assert res["failed"] == 1


# =====================================================
# process_drive_folder
# =====================================================


def test_process_drive_folder_success(monkeypatch, tmp_path):
    mock_service = object()
    fake = {"id": "1", "name": "song.mp3"}
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.get_drive_service", lambda: mock_service
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.list_music_files", lambda s, f: [fake]
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.download_file",
        lambda s, i, p: open(p, "w").write("x"),
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.rename_music_file", lambda p, o, s: p
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.upload_file", lambda s, p, d: None
    )

    res = renamer.process_drive_folder("src", "dest", "__")
    assert res["downloaded"] == 1
    assert res["uploaded"] == 1
    assert res["failed"] == 0


def test_process_drive_folder_upload_failure(monkeypatch, tmp_path):
    mock_service = object()
    fake = {"id": "2", "name": "song.mp3"}
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.get_drive_service", lambda: mock_service
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.list_music_files", lambda s, f: [fake]
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.download_file",
        lambda s, i, p: open(p, "w").write("x"),
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.rename_music_file", lambda p, o, s: p
    )
    monkeypatch.setattr(
        "music_naming_and_tagging.renamer.drive.upload_file",
        lambda s, p, d: (_ for _ in ()).throw(Exception("fail")),
    )

    res = renamer.process_drive_folder("src", "dest", "__")
    assert res["failed"] == 1
