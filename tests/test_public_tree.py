"""The public distribution must remain usable without the private parent tree."""

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = {
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "target",
    "out",
    "cache",
    "dist",
    "broadcast",
}


def test_public_links_resolve_inside_public_tree() -> None:
    failures = []
    for path in ROOT.rglob("*.md"):
        if OUTPUTS.intersection(path.relative_to(ROOT).parts):
            continue
        for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", path.read_text()):
            target = target.strip("<>")
            if target.startswith("#") or urlsplit(target).scheme:
                continue
            resolved = (path.parent / unquote(target.split("#", 1)[0])).resolve()
            if not resolved.is_relative_to(ROOT) or not resolved.exists():
                failures.append(f"{path.relative_to(ROOT)}: {target}")
    assert not failures, "\n".join(failures)


def test_public_tree_has_no_external_symlinks() -> None:
    for path in ROOT.rglob("*"):
        if OUTPUTS.intersection(path.relative_to(ROOT).parts):
            continue
        if path.is_symlink():
            assert path.resolve().is_relative_to(ROOT), str(path.relative_to(ROOT))
