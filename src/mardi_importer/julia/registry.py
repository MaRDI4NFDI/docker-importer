"""The Julia General registry: which packages are imported, and their registry facts.

Pure functions plus a clone of `JuliaRegistries/General` with its history: the
registry files name every registered version, the history tells the day each one
was registered. The registry is MIT-licensed; nothing here needs credentials or
the GitHub API.
"""

from __future__ import annotations

import re
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

GENERAL_URL = "https://github.com/JuliaRegistries/General.git"

# The registry's first commit imported METADATA.jl wholesale; the versions it
# carries were published earlier, on days the registry does not know.
INITIAL_IMPORT_SUBJECT = "Registry data generated from METADATA.jl"

# Since 2019 every registration is one main-line commit "New version: Pkg v1.2.3"
# (or "New package: ..."); in mid-2019 it was a merge of branch register/Pkg/v1.2.3.
_NEW_VERSION = re.compile(r"^New (?:version|package): (\S+) v(\d[^\s()]*)")
_REGISTER_BRANCH = re.compile(r"register/([^/\s]+)/v(\d[^\s()]*)")
_ADDED_VERSION = re.compile(r'^\+\["([^"]+)"\]')      # diff line adding a version header

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


def live_versions(versions: dict) -> list[str]:
    """Every non-yanked version, lowest first. Yanked releases were withdrawn."""
    return sorted((v for v, meta in versions.items() if not meta.get("yanked")), key=_version_key)


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True,
                          check=True).stdout.strip()


def clone_registry(dest: Path) -> Path:
    """Clone of General with its history into ``dest`` (reused if already there).

    A shallow clone left by an earlier version of this importer is deepened,
    since the version dates come from the history.
    """
    if not (dest / "Registry.toml").exists():
        subprocess.run(["git", "clone", "--quiet", GENERAL_URL, str(dest)], check=True, timeout=1800)
    elif _git(dest, "rev-parse", "--is-shallow-repository") == "true":
        subprocess.run(["git", "-C", str(dest), "fetch", "--quiet", "--unshallow"], check=True, timeout=1800)
    return dest


def read_registry(root: Path) -> tuple[str, list[dict]]:
    """All packages in a General clone. In-scope ones carry their versions.

    ``version`` is the latest live one, ``versions`` every live one, lowest first.
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
               "version": None, "versions": []}
        if in_scope(pkg):
            versions = root / entry["path"] / "Versions.toml"
            if versions.exists():
                table = tomllib.loads(versions.read_text())
                pkg["version"] = latest_version(table)
                pkg["versions"] = live_versions(table)
        packages.append(pkg)
    return sha, packages


# -- when was each version registered? ------------------------------------------------

def _utc_day(iso: str) -> str:
    """``2019-08-01T01:09:14+04:00`` → the UTC calendar day, ``2019-07-31``."""
    return datetime.fromisoformat(iso).astimezone(timezone.utc).date().isoformat()


def parse_registration_log(lines: Iterable[str]) -> dict[str, dict[str, tuple[str, str]]]:
    """``package → version → (UTC day, commit)`` from ``git log`` lines ``sha<TAB>date<TAB>subject``.

    Only commits titled like a registration count. Lines come newest first; the
    oldest commit naming a version wins, so a re-registration keeps the first day.
    """
    out: dict[str, dict[str, tuple[str, str]]] = {}
    for line in lines:
        sha, _, rest = line.partition("\t")
        date, _, subject = rest.partition("\t")
        m = _NEW_VERSION.match(subject) or _REGISTER_BRANCH.search(subject)
        if m:
            out.setdefault(m.group(1), {})[m.group(2)] = (_utc_day(date), sha)
    return out


def registration_dates(root: Path) -> dict[str, dict[str, tuple[str, str]]]:
    """Registration day of every version named by a main-line commit of the clone."""
    log = _git(root, "log", "--first-parent", "--format=%H%x09%cI%x09%s")
    return parse_registration_log(log.splitlines())


def parse_version_history(patch: str) -> dict[str, tuple[str, str, str]]:
    """``version → (UTC day, commit, subject)`` from ``git log --reverse -p`` of a Versions.toml.

    The commit whose diff first adds the ``["1.2.3"]`` header registered that
    version. Each commit in ``patch`` starts with ``\\x01sha<TAB>date<TAB>subject``.
    """
    out: dict[str, tuple[str, str, str]] = {}
    sha = day = subject = ""
    for line in patch.splitlines():
        if line.startswith("\x01"):
            sha, _, rest = line[1:].partition("\t")
            date, _, subject = rest.partition("\t")
            day = _utc_day(date)
        elif sha and (m := _ADDED_VERSION.match(line)) and m.group(1) not in out:
            out[m.group(1)] = (day, sha, subject)
    return out


def version_history(root: Path, path: str) -> dict[str, tuple[str, str, str]]:
    """When each version header entered ``<path>/Versions.toml`` on the main line."""
    patch = _git(root, "log", "--first-parent", "--reverse", "--format=%x01%H%x09%cI%x09%s",
                 "-p", "--", f"{path}/Versions.toml")
    return parse_version_history(patch)


def version_dates(root: Path, pkg: dict, index: dict) -> list[tuple[str, str | None]]:
    """``(version, registration day)`` for each live version of ``pkg``, lowest first.

    Days come from the registration commits (``index``, see
    :func:`registration_dates`). Versions older than automated registration are
    dated by the METADATA sync commit that added them to Versions.toml. Versions
    the registry's initial import already carried have no known day: ``None``.
    """
    known: dict[str, tuple[str, str]] = dict(index.get(pkg["name"], {}))
    missing = [v for v in pkg.get("versions", []) if v not in known]
    if missing:
        history = version_history(root, pkg["path"])
        for v in missing:
            if v in history and history[v][2] != INITIAL_IMPORT_SUBJECT:
                known[v] = history[v][:2]
    return [(v, known[v][0] if v in known else None) for v in pkg.get("versions", [])]

