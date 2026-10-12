"""Building blocks for the zbMATH *update* pipeline.

Two consecutive processed dumps (as produced by
:meth:`mardi_importer.zbmath.ZBMathSource.process_data`) are compared here:

``hash_dump``
    Reduce a processed dump to a small ``de_number``/``hash`` table so the
    expensive row-by-row comparison only has to look at records that changed.

``build_diff``
    For the records whose hash changed, emit one row per record holding every
    original column plus a ``<col>_new`` column carrying the new value.

``build_deleted``
    de_numbers that were in the old dump and are gone from the new one.
    Records that *appeared* are not handled here: they get a de_number above
    the import flow's watermark, and that flow downloads everything past its
    watermark, so it picks them up on its own.

Conventions
-----------
* Every value is read and written as a **string**; nothing is ever inferred by
  pandas. A dump written by ``process_data`` contains the literal ``"None"``
  for missing values, and ``read_csv`` would otherwise turn a column such as
  ``publication_year`` into ``float64`` and write ``2024.0`` back out.
* ``<col>_new`` empty means *unchanged*. A field that zbMATH **emptied** is
  marked with :data:`DELETED_SENTINEL` so the update step can tell the two
  apart and remove the stale claim.
* A new value carrying the zbMATH licence-conflict placeholder is treated as
  "no information available", never as a real value and never as a deletion.
* Files are written with ``QUOTE_NONE`` and no quote character, so a ``"`` in
  a title or review is written as it is instead of making the writer raise.
  ``process_data`` already escapes tabs, newlines and carriage returns, and any
  residue is stripped here, so the output stays parseable by both pandas and
  the plain ``split("\\t")`` reader in ``ZBMathSource.push``.
* ``publication_year`` is compared as an integer, so ``2024`` and ``2024.0``
  (what older processed dumps can contain, see ``misc.year_to_str``) are equal.
"""

from __future__ import annotations

import csv
import hashlib
import os
from typing import Callable, Iterable, Iterator, Optional, Sequence

DE_COL = "de_number"
HASH_COL = "hash"

#: Marker written into ``<col>_new`` when a field that had a value in the old
#: dump is empty in the new one.
DELETED_SENTINEL = "__ZBMATH_DELETED__"

#: Same string the importer checks for, without the trailing period so it also
#: matches the variant that carries one.
CONFLICT_STRING = (
    "zbMATH Open Web Interface contents unavailable due to conflicting licenses"
)

#: Placeholders ``process_data`` writes for "no value".
_EMPTY_VALUES = {"", "None", "nan", "NaN", "0001-01-01T00:00:00", "0001-01-01T00:00:00Z"}

#: Columns left out of the row hash, so a change in them alone does not make
#: a record a comparison candidate. ``creation_date`` is zbMATH's datestamp: the
#: time zbMATH processed the record, not when the article was published. The
#: publication date is the year, so the datestamp is not written anywhere and a
#: change to it is not worth a diff row.
DEFAULT_HASH_EXCLUDE: tuple[str, ...] = ("creation_date",)

#: Columns whose value is a semicolon-separated **set**: the order zbMATH
#: happens to list them in carries no meaning, and the claims they become carry
#: no order on the item either. Reordering one of these is not a change.
#:
#: ``author``/``author_ids`` and ``review_sign``/``reviewer_id`` are deliberately
#: NOT here. Those are two columns aligned by position, so a reordering of one
#: without the other is a genuine re-pairing of a name to an ID, and comparing
#: either column as a set on its own would hide it.
UNORDERED_COLUMNS = frozenset({
    "classifications",
    "keywords",
    "links",
    "license",
    "references",
})

#: Bumped whenever the hashing scheme changes (the digest, the separator,
#: DEFAULT_HASH_EXCLUDE, or the canonical form a value is reduced to). It is
#: part of the hash file's name, so a hash file written by an older scheme is
#: simply not found and gets recomputed, instead of being reused and making
#: every record look changed.
HASH_SCHEME = 4


def hash_path_for(processed_dump_path: str) -> str:
    """Conventional hash-file path for a processed dump."""
    return f"{processed_dump_path}.hashes-v{HASH_SCHEME}.tsv"


class ZBMathDiffError(RuntimeError):
    """Raised when the two dumps cannot be compared."""


# ── helpers ──────────────────────────────────────────────────────────────────

def is_blank(value: Optional[str]) -> bool:
    """True for values that carry no information."""
    if value is None:
        return True
    return value.strip() in _EMPTY_VALUES


def has_conflict(value: Optional[str]) -> bool:
    """True when the value is (or contains) the licence-conflict placeholder."""
    return value is not None and CONFLICT_STRING in value


def split_list(value: Optional[str]) -> list[str]:
    """A semicolon-separated field as a list: stripped, placeholders dropped.

    The single definition of how these fields split, shared with
    ``ZBMathUpdate.parse_list`` so comparison and application never disagree.
    """
    if is_blank(value):
        return []
    return [x.strip() for x in value.split(";") if x.strip() and not is_blank(x)]


#: Columns holding a year, compared after :func:`year_value` normalisation.
YEAR_COLUMNS = frozenset({"publication_year"})


def year_value(value: Optional[str]) -> str:
    """A year cell as an integer string: ``"2024.0"`` -> ``"2024"``."""
    if is_blank(value):
        return ""
    text = value.strip()
    if text.endswith(".0") and text[:-2].lstrip("+-").isdigit():
        return text[:-2]
    return text


def canonical(
    value: Optional[str], unordered: bool = False, column: Optional[str] = None
) -> str:
    """The form a field value is compared and hashed in.

    Collapses the differences that are not changes: the ``"None"`` placeholder
    against an empty cell, surrounding whitespace, a float-formatted year (for
    a column in :data:`YEAR_COLUMNS`) and — for a column listed in
    :data:`UNORDERED_COLUMNS` — the order of the values within it.
    """
    if unordered:
        return ";".join(sorted(split_list(value)))
    if column in YEAR_COLUMNS:
        return year_value(value)
    return "" if is_blank(value) else value.strip()


def _sanitise(value: Optional[str]) -> str:
    """Make a value safe for an unquoted tab-separated file."""
    if value is None:
        return ""
    return value.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _reader(path: str) -> Iterator[dict]:
    """Yield rows of a processed dump as ``{column: str}``."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            yield row


def _header(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        line = f.readline()
    if not line:
        raise ZBMathDiffError(f"{path} is empty")
    return line.rstrip("\n").split("\t")


def _durable(f) -> None:
    """Flush and fsync ``f`` so the rename that follows never publishes a
    file whose contents did not reach the disk."""
    f.flush()
    os.fsync(f.fileno())


def _tsv_writer(f):
    """Tab-separated writer that never quotes and never needs to escape.

    With ``QUOTE_NONE`` the csv module still treats ``"`` as the quote
    character and refuses to write it unless an escape character is set;
    ``quotechar=None`` turns that off, so the value goes out unchanged.
    """
    return csv.writer(
        f, delimiter="\t", quoting=csv.QUOTE_NONE, quotechar=None,
        lineterminator="\n",
    )


def _batched(seq: Sequence, size: int) -> Iterator[Sequence]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ── step 1: hashes ───────────────────────────────────────────────────────────

def hash_dump(
    processed_dump_path: str,
    hash_path: Optional[str] = None,
    exclude_cols: Iterable[str] = DEFAULT_HASH_EXCLUDE,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> str:
    """Write a ``de_number``/``hash`` table for a processed dump.

    Args:
        processed_dump_path: Processed dump written by ``process_data``.
        hash_path: Output path. Defaults to :func:`hash_path_for`.
        exclude_cols: Columns not taken into account for the hash.
        progress_callback: Called with the last hashed de_number.

    Returns:
        The path of the hash file.
    """
    hash_path = hash_path or hash_path_for(processed_dump_path)
    header = _header(processed_dump_path)
    if DE_COL not in header:
        raise ZBMathDiffError(f"{processed_dump_path} has no {DE_COL} column")

    excluded = set(exclude_cols) | {DE_COL}
    hashed_cols = [c for c in header if c not in excluded]

    tmp = hash_path + ".tmp"
    seen: set[str] = set()
    last_de = None
    with open(tmp, "w", encoding="utf-8", newline="") as out:
        writer = _tsv_writer(out)
        writer.writerow([DE_COL, HASH_COL])
        for row in _reader(processed_dump_path):
            de = (row.get(DE_COL) or "").strip()
            if not de:
                continue
            if de in seen:
                raise ZBMathDiffError(
                    f"duplicate {DE_COL} {de} in {processed_dump_path}; "
                    "the comparison assumes one row per de_number"
                )
            seen.add(de)
            digest = hashlib.blake2b(digest_size=16)
            for col in hashed_cols:
                digest.update(
                    canonical(
                        row.get(col), col in UNORDERED_COLUMNS, column=col
                    ).encode("utf-8")
                )
                digest.update(b"\x1f")
            writer.writerow([de, digest.hexdigest()])
            last_de = de
        # ``seen`` can get large; drop it before the rename
        del seen
        _durable(out)
    os.replace(tmp, hash_path)
    if progress_callback and last_de:
        progress_callback(last_de)
    return hash_path


def read_hashes(hash_path: str) -> dict[str, str]:
    """Read a hash file into ``{de_number: hash}``."""
    hashes: dict[str, str] = {}
    with open(hash_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            de = (row.get(DE_COL) or "").strip()
            if de:
                hashes[de] = (row.get(HASH_COL) or "").strip()
    return hashes


# ── step 2: the diff ─────────────────────────────────────────────────────────

def classify_change(
    old_value: Optional[str],
    new_value: Optional[str],
    unordered: bool = False,
    column: Optional[str] = None,
) -> Optional[str]:
    """Decide what belongs in ``<col>_new`` for one field.

    Args:
        old_value: The old dump's cell.
        new_value: The new dump's cell.
        unordered: True for a column in :data:`UNORDERED_COLUMNS`, where the
            same values in a different order are not a change.
        column: The column name, for column-specific normalisation (years).

    Returns the new value, :data:`DELETED_SENTINEL`, or ``None`` for
    "unchanged / nothing to do".
    """
    old_blank = is_blank(old_value)
    new_blank = is_blank(new_value)

    if new_blank:
        # Nothing in the new dump. Only a real deletion if the old dump had
        # something we could have written to the portal in the first place.
        if old_blank or has_conflict(old_value):
            return None
        return DELETED_SENTINEL

    if has_conflict(new_value):
        # zbMATH cannot tell us the value; that is not a deletion either.
        return None

    if not old_blank and canonical(old_value, unordered, column) == canonical(
        new_value, unordered, column
    ):
        return None

    if column in YEAR_COLUMNS:
        return year_value(new_value)
    return new_value.strip()


def build_diff(
    old_data_file: str,
    new_data_file: str,
    old_hash_file: str,
    new_hash_file: str,
    output_file: str,
    batch_size: int = 100_000,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> tuple[str, int]:
    """Compare two processed dumps and write the per-field differences.

    Only records present in *both* dumps whose hash differs are considered;
    records that vanished are handled by :func:`build_deleted`, and records
    that appeared are left to the import flow.

    The candidate de_numbers are processed in batches, and each batch costs one
    pass over each dump — so a run is a handful of sequential reads rather than
    one full re-read of the new dump per chunk of the old one.

    Returns:
        ``(output_file, number_of_rows_written)``.
    """
    header = _header(old_data_file)
    new_header = _header(new_data_file)
    if header != new_header:
        raise ZBMathDiffError(
            "the two dumps do not have the same columns, so a field-by-field "
            f"comparison is not possible.\n  old: {header}\n  new: {new_header}"
        )
    # Both headers are equal from here on, so one name stands for both.
    if DE_COL not in header:
        raise ZBMathDiffError(f"neither dump has a {DE_COL} column")

    value_cols = [c for c in header if c != DE_COL]
    out_header = [DE_COL] + value_cols + [f"{c}_new" for c in value_cols]

    old_hashes = read_hashes(old_hash_file)
    new_hashes = read_hashes(new_hash_file)
    candidates = sorted(
        de for de in old_hashes.keys() & new_hashes.keys()
        if old_hashes[de] != new_hashes[de]
    )
    del old_hashes, new_hashes

    tmp = output_file + ".tmp"
    written = 0
    with open(tmp, "w", encoding="utf-8", newline="") as out:
        writer = _tsv_writer(out)
        writer.writerow(out_header)

        for batch in _batched(candidates, batch_size):
            wanted = set(batch)

            # New-side rows for this batch, kept in memory (bounded by batch_size).
            new_rows: dict[str, dict] = {}
            for row in _reader(new_data_file):
                de = (row.get(DE_COL) or "").strip()
                if de in wanted:
                    new_rows[de] = row
                    if len(new_rows) == len(wanted):
                        break

            for row in _reader(old_data_file):
                de = (row.get(DE_COL) or "").strip()
                if de not in wanted:
                    continue
                new_row = new_rows.get(de)
                if new_row is None:
                    # Hashes said it is in both files; if it is not, the inputs
                    # are inconsistent and silently skipping would hide it.
                    raise ZBMathDiffError(
                        f"de_number {de} is in the hash files but not in {new_data_file}"
                    )

                changes = {
                    col: classify_change(
                        row.get(col), new_row.get(col),
                        unordered=col in UNORDERED_COLUMNS,
                        column=col,
                    )
                    for col in value_cols
                }
                if not any(v is not None for v in changes.values()):
                    continue

                writer.writerow(
                    [de]
                    + [_sanitise(row.get(col)) for col in value_cols]
                    + [_sanitise(changes[col]) for col in value_cols]
                )
                written += 1
                if progress_callback:
                    progress_callback(de)
        _durable(out)

    os.replace(tmp, output_file)
    return output_file, written


# ── step 3: vanished and brand new records ───────────────────────────────────

def build_deleted(
    old_hash_file: str,
    new_hash_file: str,
    deleted_file: str,
) -> tuple[str, int]:
    """Write the de_numbers that are in the old dump but not the new one.

    Returns:
        ``(deleted_file, count)``.
    """
    old_de = set(read_hashes(old_hash_file))
    new_de = set(read_hashes(new_hash_file))
    vanished = sorted(old_de - new_de, key=lambda x: (len(x), x))

    tmp = deleted_file + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(f"{DE_COL}\n")
        for de in vanished:
            f.write(f"{de}\n")
        _durable(f)
    os.replace(tmp, deleted_file)
    return deleted_file, len(vanished)


# ── command line ─────────────────────────────────────────────────────────────

def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the comparison from a shell, outside the Prefect flow.

    Replaces the standalone ``get_diffs.py`` / ``get_deleted_new.py`` scripts;
    the flow calls the same functions.
    """
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--old-data-file", required=True)
    parser.add_argument("--new-data-file", required=True)
    parser.add_argument(
        "--old-hash-file",
        help="defaults to the conventional hash path, computed if absent",
    )
    parser.add_argument(
        "--new-hash-file",
        help="defaults to the conventional hash path, computed if absent",
    )
    parser.add_argument("--out-dir", default=".")
    args = parser.parse_args(argv)

    old_hash = args.old_hash_file or hash_path_for(args.old_data_file)
    new_hash = args.new_hash_file or hash_path_for(args.new_data_file)
    for data_file, hash_file in ((args.old_data_file, old_hash),
                                 (args.new_data_file, new_hash)):
        if not os.path.exists(hash_file):
            print(f"hashing {data_file} -> {hash_file}")
            hash_dump(data_file, hash_path=hash_file)

    def stem(path: str) -> str:
        name = os.path.basename(path)
        for suffix in (".csv", ".txt", ".tsv"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
        return name

    tag = f"{stem(args.old_data_file)}_TO_{stem(args.new_data_file)}"
    os.makedirs(args.out_dir, exist_ok=True)

    diff_path, rows = build_diff(
        old_data_file=args.old_data_file,
        new_data_file=args.new_data_file,
        old_hash_file=old_hash,
        new_hash_file=new_hash,
        output_file=os.path.join(args.out_dir, f"differences_{tag}.csv"),
    )
    print(f"{rows} changed record(s) -> {diff_path}")

    deleted, n_deleted = build_deleted(
        old_hash_file=old_hash,
        new_hash_file=new_hash,
        deleted_file=os.path.join(args.out_dir, f"deleted_{tag}.tsv"),
    )
    print(f"{n_deleted} deleted record(s) -> {deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
