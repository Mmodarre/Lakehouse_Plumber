"""Tests for the CLAUDE.md routing-block writer/remover.

The routing block is what makes a keyword-free in-project request route to the
``lhp`` skill, so the load-bearing guarantees are: it is idempotent (writing
twice leaves one block), non-destructive (it never disturbs content the user
wrote around it), and cleanly reversible (uninstall leaves no trace).
"""

from pathlib import Path

from lhp.cli import _claude_setup as cs

CLAUDE_MD = "CLAUDE.md"


def _read(tmp: Path) -> str:
    return (tmp / CLAUDE_MD).read_text(encoding="utf-8")


def test_write_creates_file_when_absent(tmp_path: Path) -> None:
    status = cs.write_routing_block(tmp_path)
    assert status == "created"
    text = _read(tmp_path)
    assert cs.ROUTING_START in text
    assert cs.ROUTING_END in text
    assert "Lakehouse Plumber project" in text


def test_write_is_idempotent(tmp_path: Path) -> None:
    assert cs.write_routing_block(tmp_path) == "created"
    # A second write detects the block is already current and changes nothing.
    assert cs.write_routing_block(tmp_path) == "unchanged"
    text = _read(tmp_path)
    # Exactly one block — not appended twice.
    assert text.count(cs.ROUTING_START) == 1
    assert text.count(cs.ROUTING_END) == 1


def test_write_appends_to_existing_file_without_block(tmp_path: Path) -> None:
    user_content = "# My Project\n\nSome notes I wrote.\n"
    (tmp_path / CLAUDE_MD).write_text(user_content, encoding="utf-8")

    status = cs.write_routing_block(tmp_path)
    assert status == "updated"
    text = _read(tmp_path)
    # User content is preserved verbatim and the block is appended after it.
    assert text.startswith(user_content)
    assert cs.ROUTING_START in text
    assert text.index("My Project") < text.index(cs.ROUTING_START)


def test_write_replaces_stale_block_in_place(tmp_path: Path) -> None:
    head = "# My Project\n\nIntro.\n\n"
    tail = "\n\n## My own footer\nKeep me.\n"
    stale = f"{cs.ROUTING_START}\nOLD CONTENT\n{cs.ROUTING_END}"
    (tmp_path / CLAUDE_MD).write_text(head + stale + tail, encoding="utf-8")

    status = cs.write_routing_block(tmp_path)
    assert status == "updated"
    text = _read(tmp_path)
    # Surrounding content survives; the stale body is gone; one block remains.
    assert "Intro." in text
    assert "My own footer" in text
    assert "OLD CONTENT" not in text
    assert text.count(cs.ROUTING_START) == 1


def test_remove_deletes_block_and_preserves_surrounding(tmp_path: Path) -> None:
    head = "# My Project\n\nIntro.\n"
    (tmp_path / CLAUDE_MD).write_text(head, encoding="utf-8")
    cs.write_routing_block(tmp_path)

    status = cs.remove_routing_block(tmp_path)
    assert status == "removed"
    text = _read(tmp_path)
    assert "Intro." in text
    assert cs.ROUTING_START not in text


def test_remove_deletes_file_when_lhp_created_it(tmp_path: Path) -> None:
    # LHP created the file (block-only), so removing the block removes the file.
    cs.write_routing_block(tmp_path)
    status = cs.remove_routing_block(tmp_path)
    assert status == "removed"
    assert not (tmp_path / CLAUDE_MD).exists()


def test_remove_is_absent_when_no_file(tmp_path: Path) -> None:
    assert cs.remove_routing_block(tmp_path) == "absent"


def test_remove_is_absent_when_no_block(tmp_path: Path) -> None:
    (tmp_path / CLAUDE_MD).write_text("# Just my notes\n", encoding="utf-8")
    assert cs.remove_routing_block(tmp_path) == "absent"
    # The user's file is left untouched.
    assert _read(tmp_path) == "# Just my notes\n"
