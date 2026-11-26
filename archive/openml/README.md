# Archive: earlier OpenML importer

Files from the earlier OpenML importer (2023–2025) that the current importer
in `src/mardi_importer/openml/` no longer uses. They are kept here for
reference only: this folder is outside `src/`, so it is not installed or
imported, and nothing here is run.

- `OpenMLPublication.py` created a publication item from a DOI or arXiv ID.
  The current importer creates papers through the Crossref- and
  arXiv-Importer instead. Note: it wrote arXiv IDs into the DOI property.
- `misc.py` had `convert_time_to_iso()` for the upload date. The current
  importer keeps the day from OpenML's `upload_date` directly.
