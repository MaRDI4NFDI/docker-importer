"""The Julia General registry: which packages are imported, and their registry facts.

Pure functions plus a shallow clone of `JuliaRegistries/General`. The registry is
MIT-licensed; nothing here needs credentials or the GitHub API.
"""

from __future__ import annotations

import re
import subprocess
import tomllib
from pathlib import Path

GENERAL_URL = "https://github.com/JuliaRegistries/General.git"

# Scope: packages whose repository lives under one of these GitHub organisations.
SEED_ORGS = ("SciML", "JuliaMath", "JuliaLinearAlgebra", "JuliaNLSolvers", "JuliaDiff")

# Packages registered in a sub-directory of a shared repository are internal
# splits of a larger package and are skipped — except these, cited in their own right.
SUBDIR_ALLOWLIST = frozenset({"StochasticDiffEq", "DelayDiffEq"})


def repo_key(url: str) -> str | None:
    """Comparison key for a GitHub URL: ``github.com/owner/repo``, lower-cased.

    GitHub paths are case-insensitive, and the knowledge graph holds URLs with
    ``.git``, trailing slashes and ``/blob/...`` suffixes, so the raw string is
    not a key.
    """
    m = re.search(r"github\.com[/:]([^/\s]+)/([^/\s#?]+)", (url or "").strip().lower())
    if not m:
        return None
    return f"github.com/{m.group(1)}/{m.group(2).removesuffix('.git')}"


def org_of(url: str) -> str | None:
    key = repo_key(url)
    return key.split("/")[1] if key else None


def in_scope(pkg: dict) -> bool:
    """Seed organisation, no ``_jll`` binary wrapper, no sub-directory package
    unless allow-listed."""
    if pkg["name"].endswith("_jll"):
        return False
    if (org_of(pkg.get("repo", "")) or "") not in {o.lower() for o in SEED_ORGS}:
        return False
    return not pkg.get("subdir") or pkg["name"] in SUBDIR_ALLOWLIST


def _version_key(v: str) -> tuple:
    core, _, pre = v.partition("-")
    nums = tuple(int(x) for x in re.findall(r"\d+", core)[:3])
    return nums + (0 if pre else 1,)


def latest_version(versions: dict) -> str | None:
    """Highest non-yanked version; a release beats a pre-release of the same core."""
    live = [v for v, meta in versions.items() if not meta.get("yanked")]
    return max(live, key=_version_key) if live else None


def clone_registry(dest: Path) -> Path:
    """Shallow clone of General into ``dest`` (reused if already there)."""
    if not (dest / "Registry.toml").exists():
        subprocess.run(["git", "clone", "--depth", "1", "--quiet", GENERAL_URL, str(dest)],
                       check=True, timeout=600)
    return dest


def read_registry(root: Path) -> tuple[str, list[dict]]:
    """All packages in a General clone. In-scope ones carry their latest version.

    Returns the registry commit, which pins every registry reference URL.
    """
    sha = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                         capture_output=True, text=True, check=True).stdout.strip()
    reg = tomllib.loads((root / "Registry.toml").read_text())["packages"]
    packages = []
    for uuid, entry in reg.items():
        pkg_toml = tomllib.loads((root / entry["path"] / "Package.toml").read_text())
        pkg = {"name": entry["name"], "uuid": uuid, "path": entry["path"],
               "repo": pkg_toml.get("repo", ""), "subdir": pkg_toml.get("subdir"),
               "version": None}
        if in_scope(pkg):
            versions = root / entry["path"] / "Versions.toml"
            if versions.exists():
                pkg["version"] = latest_version(tomllib.loads(versions.read_text()))
        packages.append(pkg)
    return sha, packages


def registry_url(sha: str, pkg: dict, fname: str) -> str:
    """Reference URL for a registry fact, pinned to the registry commit."""
    return f"https://github.com/JuliaRegistries/General/blob/{sha}/{pkg['path']}/{fname}"
