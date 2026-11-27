# MaRDI Importer: Graph Schema and Data Flows

The docker-importer writes Items into the MaRDI portal from nine sources. Some imports run regularly, others when someone starts them, and people add and correct items by hand. Before you write a bot that creates, changes or merges items, read what a bot must never do and the section for each item type you touch.

## Contents

1. [How items get into the graph](#how-items-get-into-the-graph)
   1. [Imported on a schedule](#imported-on-a-schedule)
   2. [Imported automatically, started by hand](#imported-automatically-started-by-hand)
   3. [Added by people](#added-by-people)
2. [What a bot must never do](#what-a-bot-must-never-do)
   1. [Merge an arXiv preprint with its published version](#merge-an-arxiv-preprint-with-its-published-version)
   2. [Remove values nobody asked you to remove](#remove-values-nobody-asked-you-to-remove)
   3. [Merge person items because the names match](#merge-person-items-because-the-names-match)
3. [How values are normalized](#how-values-are-normalized)
4. [Item types and their properties](#item-types-and-their-properties)
   1. [Publication](#publication)
   2. [arXiv preprint](#arxiv-preprint)
   3. [Person](#person)
   4. [Journal, book, proceedings](#journal-book-proceedings)
   5. [R package](#r-package)
   6. [Julia package](#julia-package)
   7. [Zenodo resource](#zenodo-resource-dataset-software-report-and-others)
   8. [OpenML dataset](#openml-dataset)
   9. [MathModDB items](#mathmoddb-items)
   10. [Research theme](#research-theme)
   11. [Smaller item types](#smaller-item-types)
   12. [Vocabulary: classes, properties and profile items](#vocabulary-classes-properties-and-profile-items)
5. [Importers and bots](#importers-and-bots)
   1. [zbMATH-Importer](#zbmath-importer)
      1. [Import flow](#import-flow)
      2. [Update flow](#update-flow)
   2. [CRAN-Importer](#cran-importer)
   3. [Zenodo-Importer](#zenodo-importer)
   4. [Research-Theme-Updater](#research-theme-updater)
   5. [Julia-Importer](#julia-importer)
   6. [OpenML-Importer](#openml-importer)
   7. [Crossref-Importer and arXiv-Importer](#crossref-importer-and-arxiv-importer)
   8. [Wikidata-Importer](#wikidata-importer)
   9. [polyDB-Importer](#polydb-importer)
   10. [Item API](#item-api)
   11. [Correction scripts](#correction-scripts-bot_scripts)
6. [Writing a new bot](#writing-a-new-bot)
   1. [Checklist](#checklist)
   2. [Schema changes](#schema-changes)

## How items get into the graph

### Imported on a schedule

| Importer | Schedule (Europe/Berlin) |
| --- | --- |
| CRAN-Importer: new packages, archive status, task views | daily 02:00 |
| Zenodo-Importer: MaRDI communities | daily 03:00 |
| Research-Theme-Updater: papers for research themes | daily 02:00 |

### Imported automatically, started by hand

| Importer | Started from | Still used |
| --- | --- | --- |
| zbMATH-Importer, import flow | Prefect UI | yes |
| zbMATH-Importer, update flow | Prefect UI | yes |
| Julia-Importer | CLI `import-julia` | ? |
| polyDB-Importer | `scripts/import.py --mode polydb` | no |
| OpenML-Importer, import flow | Prefect UI | not yet |
| OpenML-Importer, update flow | Prefect UI | not yet |
| Wikidata-Importer | API or CLI | yes |

### Added by people

People add and correct items in the portal by hand. Importers and bots should be written so that they keep these edits.

| Topic | Responsible |
| --- | --- |
| MathModDB: mathematical models, formulas, quantities, research problems, computational tasks, academic disciplines | MaRDI Task Area 4 |
| Research themes | Tim Conrad |

MathModDB items are created through a menu in the portal (`createPreDefMenu_common.js`, loaded in MediaWiki:Common.js). The menu fills in instance of and MaRDI profile type, checks for a duplicate label and requires a login. See [MathModDB items](#mathmoddb-items).

## What a bot must never do

### Merge an arXiv preprint with its published version

An arXiv preprint and the article published from it are two things and therefore should be two different items. The preprint item points to the publication with *preprint of* (P1676). Do not merge them, and do not copy one item's identifiers onto the other. The integrity of the edits done by the zbMATH importer and all bots derived from it depend on them being separate.

### Remove values nobody asked you to remove

If a property has several values, only remove the ones you are explicitly asked to remove, never use REPLACE\_ALL. That would overwrite manual edits and edits done by other bots. The only exception to this is if you are explicitly asked to replace all values, for example because there can only be one correct value.

This happens easily by accident in code:

- In WikibaseIntegrator, `item.claims.add(claim)` replaces all existing values of that property, because its default is `ActionIfExists.REPLACE_ALL`. Pass `ActionIfExists.APPEND_OR_REPLACE` or `ActionIfExists.FORCE_APPEND`, or use `item.add_claims(...)`, whose default appends.
- In mardiclient, `item.add_claim(prop, value, action="replace_all")` does the same. Leave `action` at its default.
- Building an item's statements from scratch and writing it over the existing item wipes everything the importers and people added.

A few importers replace all values of properties they own, such as the CRAN-Importer for licenses and dependencies, and the Zenodo-Importer for authors and links to related works on its own items. The Research-Theme-Updater removes the oldest papers from a research theme once it has more than 100. When it links an author of a paper to a person item, it also removes any author name string on that paper with the same name, including ones that other importers or people wrote (see its section).

### Merge person items because the names match

Different mathematicians share names. Merge only on a shared identifier (ORCID, zbMATH author ID), and log the rest as possible duplicates.

## How values are normalized

The importers write identifiers and dates in one form. Lookups compare exact strings, so a value written in another form is not found, and the next importer creates a duplicate.

| Value | Stored as |
| --- | --- |
| DOI | upper case, without resolver URL (`10.1007/S00211-020-01234-5`, `10.48550/ARXIV.<arXiv ID>`, etc.) |
| arXiv ID | without the "arXiv:" prefix: `2101.01234` |
| ORCID | the bare 16-character ID, no URL: `0000-0002-1825-0097` |
| Date | `+YYYY-MM-DDT00:00:00Z` with precision year (9), month (10) or day (11); a year is `+YYYY-00-00T00:00:00Z` |
| Person label | "First (Middle) Last"; see Person for each importer's details |

The Crossref-Importer does not upper-case DOIs itself; the CRAN- and Julia-Importer do it before they call it. A new bot that calls the Crossref code directly has to do the same.

## Item types and their properties

*MaRDI profile type* (P1460) decides which profile page the portal shows for an item.

### Publication

- Instance of: scholarly article (Q56887). The Crossref-Importer uses other classes for other Crossref types: book, monograph, proceedings article, book chapter, report, dissertation, dataset.
- Label: the title.
- Description: "scientific article; zbMATH DE number \<DE\_NR>" (zbMATH-Importer), the type such as "scientific article" or "book chapter" (Crossref-Importer). For zbMATH records, the Research-Theme-Updater writes the same description as the zbMATH-Importer; its other new items get none.
- MaRDI profile type: MaRDI publication profile.

| Property | Written by |
| --- | --- |
| instance of (P31) | zbMATH-, Crossref-, Zenodo-, polyDB-Importer; Research-Theme-Updater |
| title (P159) | zbMATH-Importer; Research-Theme-Updater |
| DOI (P27) | zbMATH-, Crossref-, Zenodo-, polyDB-Importer; Research-Theme-Updater |
| zbMATH DE Number (P1451) | zbMATH-Importer; Research-Theme-Updater |
| zbMATH Open document ID | zbMATH-Importer; Research-Theme-Updater |
| author (P16) | zbMATH-, Crossref-, Zenodo-, polyDB-Importer; Research-Theme-Updater |
| author name string (P43) | zbMATH-, Crossref-, Zenodo-, polyDB-Importer; Research-Theme-Updater |
| published in | zbMATH-, Crossref-, polyDB-Importer; Research-Theme-Updater |
| volume, issue, page(s) | Crossref-, polyDB-Importer |
| ISBN-13, ISBN-10 | Crossref-, polyDB-Importer |
| publication date (P28) | zbMATH-, Crossref-, Zenodo-, polyDB-Importer; Research-Theme-Updater |
| Mathematics Subject Classification ID (P226) | zbMATH-Importer; Research-Theme-Updater |
| zbMATH Keywords | zbMATH-Importer; Research-Theme-Updater |
| review text | zbMATH-Importer |
| reviewed by | zbMATH-Importer |
| full work available at URL | zbMATH-Importer |
| copyright license (P163) | zbMATH-, Zenodo-Importer; Research-Theme-Updater |
| cites work (P223) | zbMATH-, Zenodo-Importer; Research-Theme-Updater |
| described by source | Zenodo-Importer |
| based on | Zenodo-Importer |
| part of | Zenodo-Importer |
| has part(s) | Zenodo-Importer |
| MaRDI profile type | zbMATH-, Crossref-, Zenodo-Importer; Research-Theme-Updater |
| TL;DR (P1963) | Research-Theme-Updater |
| keywords (P1964) | Research-Theme-Updater |
| OpenAlex ID (P388) | Research-Theme-Updater |
| open access status (P1987) | Research-Theme-Updater |
| OpenAlex concepts (P1988) | Research-Theme-Updater |

How values are stored:

- Title (Crossref-Importer): HTML tags such as `<i>` removed, runs of spaces collapsed.
- Publication date: zbMATH-Importer writes the year only (`+YYYY-00-00T00:00:00Z`, precision year). Crossref-Importer writes day, month or year, whatever Crossref has.
- Full work available at URL: only `http` and `https` links.
- Text from zbMATH (zbMATH-Importer): a line break or tab, with the spaces around it, becomes one space. Titles and review texts from older imports might still contain them as literal escapes such as `\N`, `\T` or `\NEWLINE`; these might be removed by separate correction scripts.

### arXiv preprint

A separate item from the published version (see What a bot must never do).

- Instance of: (Q56887)
- Label: the title.
- Description: "scientific article from arXiv".
- MaRDI profile type: MaRDI publication profile

| Property | Written by |
| --- | --- |
| instance of (P31) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| MaRDI profile type | zbMATH-, arXiv-Importer; Research-Theme-Updater |
| arXiv ID (P21) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| arXiv classification (P22) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| Mathematics Subject Classification ID (P226) | arXiv-Importer; zbMATH-Importer; Research-Theme-Updater |
| publication date (P28) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| author (P16) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| author name string (P43) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| DOI (P27) | zbMATH-, arXiv-, polyDB-Importer; Research-Theme-Updater |
| preprint of (P1676), to the publication | zbMATH-Importer; Research-Theme-Updater |
| TL;DR (P1963) | Research-Theme-Updater |
| keywords (P1964) | Research-Theme-Updater |
| OpenAlex ID (P388) | Research-Theme-Updater |
| open access status (P1987) | Research-Theme-Updater |
| OpenAlex concepts (P1988) | Research-Theme-Updater |

How values are stored:

- arXiv ID: without the "arXiv:" prefix, e.g. `2101.01234`. The zbMATH-Importer takes it from the zbMATH identifier, after the last `:`.
- DOI: `10.48550/ARXIV.<arXiv ID>`, upper case.
- MSC IDs: the arXiv-Importer keeps only arXiv categories that look like MSC codes (`65N30`, `35-XX`); ACM codes are dropped.

### Person

- Instance of: human (`wd:Q5`).
- Label: the person's name, formatted as described below.
- Description: none.
- MaRDI profile type: MaRDI person profile. The polyDB-Importer writes none.

| Property | Written by |
| --- | --- |
| instance of (P31) | zbMATH-, Crossref-, arXiv-, Zenodo-, CRAN-, Julia-, polyDB-Importer |
| zbMATH author ID | zbMATH-Importer |
| ORCID iD | Crossref-, arXiv-, Zenodo-, CRAN-, Julia-, polyDB-Importer |
| arXiv author ID | arXiv-, polyDB-Importer |
| employer | polyDB-Importer |
| aliases | Julia-Importer (new items), polyDB-Importer |
| MaRDI profile type | zbMATH-, Crossref-, arXiv-, Zenodo-, CRAN-, Julia-Importer |

How names are stored, as label and as author name string:

- zbMATH-Importer: zbMATH gives "Last, First". It is stored as "First Last": everything after the first comma, then everything before it.
- Crossref-Importer: "Given Family" from Crossref, each word capitalized, then normalized by the nameparser library.
- arXiv-Importer: the name from the arXiv API, normalized by nameparser.
- Zenodo-Importer: Zenodo gives "Family, Given"; nameparser turns it into "Given Family" with normal capitalization.
- CRAN-Importer: given and family name from Authors@R joined with a space; quotes and backslashes removed, spaces collapsed, Unicode normalized (NFC). A free-text Author field is used as written.
- Julia-Importer: the most frequent spelling across all packages that contains a space; the other spellings become aliases.
- polyDB-Importer: names come from three places: the 28 authors written into `PolyDBSource.py`, the authors, maintainers and contributors in each collection's polyDB metadata, and the authors of the papers the collections cite. Every name is normalized by nameparser. Spellings that look like the same person are then joined: same ORCID, same first and last name (ignoring case and hyphens), or an initial plus the same last name. The joined person's label is built from the longest first, middle and last name found among the spellings, and the other spellings become aliases. This is also written to person items that already exist.
- OpenML-Importer: creators and contributors are only name strings, with *object has role* creator or contributor.

An existing person's label changes in one place only: the polyDB-Importer rewrites label and aliases when it joins name variants of one of its authors. The zbMATH-Importer sets a person's label only when it creates the item.

### Journal, book, proceedings

- Instance of: scientific journal (`wd:Q5633421`), book (`wd:Q571`) or proceedings (`wd:Q1143604`).
- Label: the name, with runs of spaces collapsed.
- Description: "scientific journal" (zbMATH-Importer), none otherwise.
- MaRDI profile type: none.

| Property | Written by |
| --- | --- |
| instance of (P31) | zbMATH-, Crossref-, polyDB-Importer |
| ISSN, with distribution format print or online | Crossref-, polyDB-Importer |
| ISBN | Crossref-, polyDB-Importer (books) |
| publication date | Crossref-, polyDB-Importer (proceedings) |

### R package

- Instance of: R package (Q57080).
- Label: the package name.
- Description: the CRAN title, plus " (R Package)" when title and name are equal.
- MaRDI profile type: MaRDI software profile.
- Written by: CRAN-Importer only.

| Property | Notes |
| --- | --- |
| programmed in: R |  |
| description (long text) | replaced on update |
| last update | replaced on update |
| software version identifier | every version, with publication date |
| author, author name string | added, never removed |
| maintained by | removed only when CRAN names another maintainer |
| copyright license | replaced on update |
| depends on software | version as qualifier; replaced on update |
| imports | version as qualifier; replaced on update |
| cites work | replaced on update |
| CRAN project | end time when the package left CRAN |
| part of | the CRAN task view; core packages with subject has role |
| Wikidata QID | only when Wikidata has exactly one match |

How values are stored:

- Description: the first paragraph of the CRAN page, with line breaks and tabs removed.
- Versions: from the archive file names (`pkg_1.2-3.tar.gz` becomes `1.2-3`)
- License: CRAN's license field is split at "|" and "+" into one statement per license, mapped to license items through a fixed table ("GPL-2" and "GNU General Public License version 2" are the same item). Text in round or square brackets becomes a qualifier. "file LICENSE" becomes the File License item, with a link to the package's LICENSE file as qualifier.
- Dependencies: the version requirement in brackets, such as `>= 3.5`, becomes a qualifier.
- Dependencies not yet in the portal get a minimal R package item (instance of, programmed in R, profile type).

### Julia package

- Instance of: Julia package (Q7367514).
- Label: "Name.jl". Alias: "Name".
- Description: "Julia package".
- MaRDI profile type: MaRDI software profile.
- Written by: Julia-Importer only; updates only add.

| Property | Notes |
| --- | --- |
| Julia General registry package name | without `.jl`; the key the importer finds the item by |
| programmed in: Julia |  |
| source code repository |  |
| software version identifier | each version, with its registration day |
| copyright license | a package with an unclear license is not written at all |
| author, author name string |  |
| depends on software | only to packages already in the portal |
| described by source | cited DOIs and arXiv IDs |
| Wikidata QID |  |

### Zenodo resource (dataset, software, report and others)

- Instance of: from the Zenodo resource type, see the table below. A type not in the table gets no instance of.
- Label: the title.
- Description: "Dataset published at Zenodo repository.", with the type name. When that pair already exists, the publication date or record ID is appended. If that pair exists too, the record is not imported.
- MaRDI profile type: from the resource type, see the table below.
- Written by: Zenodo-Importer only. One item per Zenodo concept, covering all versions.

| Zenodo resource type | instance of | MaRDI profile type |
| --- | --- | --- |
| Dataset | Q56885 | dataset profile |
| Software | `Q56614` | software profile |
| Report | `Q57006` | publication profile |
| Preprint | `Q159099` | publication profile |
| Journal article | `Q56887` | publication profile |
| Book | `Q56401` | publication profile |
| Workflow | `Q68657` | workflow profile |
| Presentation | `Q56867` | none |
| Poster | `Q56854` | none |
| Figure | `Q56862` | none |
| Video/Audio | `Q56958` | none |
| Lesson | Q56846 | none |

| Property | Notes |
| --- | --- |
| Zenodo ID (P227) | the 20 newest versions, with edition and publication date |
| DOI (P27) | the concept DOI |
| description (long text) |  |
| publication date |  |
| author, author name string | replaced on update; see Zenodo-Importer for how people are matched |
| copyright license | CC BY 4.0, CC BY-SA 4.0, CC BY-NC-SA 4.0 and MIT only |
| dataset version identifier | datasets |
| software version identifier | software |
| community | added, never removed |
| Internal Project ID | MATH+ projects |
| described by source | related works with Zenodo relation isDescribedBy or isDocumentedBy; replaced on update |
| cites work | relation cites or references; replaced on update |
| based on | relation isDerivedFrom; replaced on update |
| part of | relation isPartOf; replaced on update |
| has part(s) | relation hasPart; replaced on update |

How values are stored: the description has HTML tags and entities removed; the DOI is upper case.

### OpenML dataset

- Instance of: dataset (`wd:Q1172284`).
- Label: the OpenML dataset name.
- Description: "OpenML dataset with id \<OPENML\_ID>".
- MaRDI profile type: MaRDI dataset profile. Items written by the earlier importer had an item labelled "Dataset"; the update flow replaces it.
- Written by: OpenML-Importer only.

| Property | Notes |
| --- | --- |
| OpenML dataset ID |  |
| dataset version identifier |  |
| description (long text) | on one line: line breaks and tabs become spaces; written in full, not shortened, like the zbMATH review text |
| author name string | creators and contributors, with object has role creator or contributor; OpenML gives names only, no identifiers |
| collection date | text, as OpenML gives it |
| upload date | date only, without the time of day |
| copyright license | only CC0, CC BY 4.0, CC BY-SA 4.0, CC BY-NC 4.0, CC BY-NC-SA 4.0, MIT, GPL-2 and GPL-3; other licences, such as OpenML's "Public", are not written |
| full work available at URL | ARFF download URL, Parquet download URL and original data URL |
| default target attribute | name of the column that machine-learning tasks predict by default, e.g. "class"; can be several names, comma separated; plain text |
| row id attribute | name of the column that holds the row IDs, if the dataset has one; plain text |
| OpenML semantic tag | only from the fixed list of OpenML subject tags |
| cites work | the paper, when OpenML's paper\_url contains a DOI or an arXiv ID; created through the Crossref- or arXiv-Importer if missing |
| citation text | OpenML's paper\_url field as it is, a URL or a free-text citation; written even when the paper is linked with cites work |
| checksum | MD5, with determination method MD5 |
| number of binary features, number of classes, number of features, number of instances, number of instances with missing values, number of missing values, number of numeric features, number of symbolic features | whole numbers. The dataset list does not give the number of binary features, so it is fetched per dataset (`/data/qualities/<id>`), or taken from the previous download on the volume, since a dataset's data does not change under its ID |
| file format | ARFF or Sparse ARFF, plus Apache Parquet when OpenML offers a Parquet download; other formats are not written |

### MathModDB items

MathModDB is an ontology for mathematical models, built by MaRDI Task Area 4. Its items in the portal are entered by hand.

- Instance of and MaRDI profile type: set by the creation menu, see the table below.
- Community: MathModDB (`Q6534265`), required on every MathModDB item.
- Label: the menu refuses a label that already exists.
- Written by: people, through the creation menu. No importer writes these items.

| Type | instance of | MaRDI profile type | EntitySchema |
| --- | --- | --- | --- |
| Academic discipline | `Q60231` | academic discipline profile (`Q6534268`) | E13 |
| Formula | `Q96183` | formula profile (`Q5981696`) | E5 |
| Mathematical model | `Q68663` | model profile (`Q6534270`) | E9 |
| Quantity | `Q6534237` | quantity profile (`Q6534271`) | E12 |
| Kind of quantity | `Q6534245` | quantity profile (`Q6534271`) | E17 |
| Research problem | `Q6534292` | research problem profile (`Q6534269`) | E10 |
| Computational task | `Q6534247` | task profile (`Q6534272`) | E11 |

Each type has an EntitySchema in the portal (`EntitySchema:E…`) that says what an item of that type must and may have; its page can check an item against it. Every MathModDB item must have its instance of and *community* MathModDB. Everything else is optional.

Models, formulas and tasks can also be an instance of a subtype. The class item lists them under *specialized by*:

| Type | Subtypes |
| --- | --- |
| Mathematical model | deterministic, probabilistic, dimensional, dimensionless, dynamic, static, linear, nonlinear, continuous-space, discrete-space, continuous-time, discrete-time model |
| Formula | the same twelve kinds of formula; a formula in a model or task may also be a partial differential equation (`Q6822213`) |
| Computational task | linear, nonlinear, continuous-space, discrete-space, continuous-time, discrete-time task |

How MathModDB items link to each other, according to the schemas:

| From | Property | To | Qualifiers |
| --- | --- | --- | --- |
| Academic discipline | contains (P1560) | research problem | |
| Academic discipline | specialized by (P1684) | academic discipline | |
| Research problem | modelled by (P1513) | mathematical model | |
| Research problem | specialized by (P1684) | research problem | assumes (P1674): any item |
| Mathematical model | contains (P1560) | mathematical model or formula | for a formula: role, series ordinal (P146) |
| Mathematical model | used by (P147) | computational task | |
| Mathematical model | assumes (P1674) | formula | |
| Mathematical model | approximated by (P1655), discretized by (P1656), linearized by (P1657), specialized by (P1684) | mathematical model | |
| Formula | contains (P1560) | formula or quantity | for a formula: role, series ordinal (P146) |
| Formula | in defining formula (P983) | a symbol, written as a formula | symbol represents (P984): the item the symbol stands for, such as a quantity |
| Formula | assumes (P1674) | formula | |
| Formula | approximated by (P1655), discretized by (P1656), linearized by (P1657), nondimensionalized by (P1658), specialized by (P1684) | formula | |
| Quantity | contains (P1560) | quantity or quantity kind | role |
| Quantity | approximated by (P1655), linearized by (P1657) | quantity | |
| Quantity | nondimensionalized by (P1658), specialized by (P1684) | quantity or quantity kind | |
| Quantity kind | contains (P1560) | quantity or quantity kind | role |
| Quantity kind | nondimensionalized by (P1658) | quantity or quantity kind | |
| Quantity kind | specialized by (P1684) | quantity | not yet in schema E17 |
| Computational task | contains (P1560) | computational task | order number (P1569) |
| Computational task | contains (P1560) | formula | role, series ordinal (P146), corresponds to (P1869) |
| Computational task | contains (P1560) | quantity or quantity kind | role, corresponds to (P1869) |
| Computational task | assumes (P1674) | formula | |
| Computational task | approximated by (P1655), discretized by (P1656), linearized by (P1657), specialized by (P1684) | computational task | |
| Any of these | described by source (P286) | publication | role |

The role is given with the qualifier *object has role* (P560). The allowed roles depend on what is linked:

- Formula in a model or task: governing equation (`Q7270342`), initial condition (`Q6534264`), final condition (`Q6534267`), boundary condition (`Q6534259`), coupling condition (`Q6534266`), computational domain (`Q6775579`), analytical solution (`Q6775641`), weak formulation (`Q6775618`), strong formulation (`Q6775640`), constraint (`Q6534262`), constitutive equation (`Q6775587`), input (`Q6480299`), output (`Q6480295`), parameter (`Q6480341`), constant (`Q6480317`), objective function (`Q6534255`).
- Formula in a formula: the conditions, computational domain, analytical solution, weak and strong formulation, constraint and constitutive equation from above, and physical law (`Q6775588`).
- Quantity or quantity kind: input (`Q6480299`), output (`Q6480295`), parameter (`Q6480341`), constant (`Q6480317`), objective function (`Q6534255`), analytical solution (`Q6775641`).
- Publication: review (`Q6672366`), use (`Q6480405`), invention (`Q6672344`), documentation (`Q6672349`), science project (`Q6775621`), analysis (`Q6825292`), application (`Q6825298`).

The MathModDB team also names definition (`Q7268735`) as a formula role; the schemas do not list it yet.

How values are stored:

- Formula: *defining formula* (P989) holds the whole formula and *in defining formula* (P983) holds each symbol in it, both as formulas. A formula has both or neither.
- Publication: a linked publication must be an instance of one of these: scholarly article, conference paper, chapter, monograph, thesis, dissertation, book, report, scientific journal, version, edition or translation, ISO standard edition, YouTube channel, written work, volume, reference work, encyclopedic dictionary, Jewish encyclopedia, universal encyclopedia, literary work, controlled vocabulary, explanatory dictionary. The MathModDB team creates these publications by hand on the portal page Special:ImportFromPid, which runs the importer's DOI import as a Prefect flow. It passes each DOI to the arXiv-, Zenodo- or Crossref-Importer, so the publication items look like theirs.
- Many items also have *Description (long)* (P1459) and *Wikidata QID* (P12).

Other properties, used on items of any type:

| Property | Used for |
| --- | --- |
| named after (P558) | the person the item is named after, e.g. Gaussian-process regression model is named after Carl Friedrich Gauss. MathModDB started using it to connect to FactGrid, where these people have items too (Gauss is FactGrid `Q750567`). |
| similar to (P1691) | another item that is similar, where neither is a generalization of the other (for that, use *specialized by*) |
| described at URL (P104) | a web page about exactly this item |
| related URL (P1690) | a web page related to the item, but not exactly about it |

### Research theme

Research themes are created by hand (see Added by people). The Research-Theme-Updater adds new papers to them every night.

- Instance of: research theme (`Q7266523`).
- Label and description: the theme's name and what it covers. The Research-Theme-Updater's LLM reads both to decide which papers belong to the theme.
- Written by: people, except *has part(s)* and *last update*.

| Property | Notes |
| --- | --- |
| arXiv search query (P1965) | set by hand |
| OpenAlex query (P1967) | set by hand |
| zbMATH query (P1979) | set by hand |
| search window in days (P1968) | set by hand; 10 days if missing |
| auto-classify keyword (P1990) | set by hand; a paper whose title or abstract contains one goes into the theme without asking the LLM |
| has part(s) (P265) | papers, added by the Research-Theme-Updater, and on some themes datasets, added once by a maintenance script; trimmed to 100 where possible (see Research-Theme-Updater) |
| last update (P170) | set once by the maintenance script that added the datasets |

### Smaller item types

| Item type | Written by | Main properties |
| --- | --- | --- |
| CRAN task view | CRAN-Importer | CRAN task view ID, part of CRAN Task Views, website, repository, author, last update |
| Zenodo community | Zenodo-Importer | instance of community, Zenodo community ID, described at URL, MaRDI community profile |
| Project (MATH+) | Zenodo-Importer | instance of scientific project, community |
| polyDB collection | polyDB-Importer | author, maintained by, contributed by, cites work, data and repository URLs |
| Workflow | item API, Zenodo-Importer | item API: research workflow and script-based workflow, problem statement, uses, author, stored at, MaRDI workflow profile. Zenodo-Importer: research workflow, MaRDI workflow profile and the properties every Zenodo resource gets |

### Vocabulary: classes, properties and profile items

Classes and properties that exist in Wikidata are copied from there by the Wikidata-Importer and carry *Wikidata QID* or *Wikidata PID*. Each importer lists the ones it needs in its `wikidata_entities.txt`. Portal-only properties and items (MaRDI profile type, the profile items, Julia package, zbMATH DE Number and others) are created by each importer's setup from `new_entities.json`, and code refers to them by their English label.

## Importers and bots

### zbMATH-Importer

One importer with two Prefect flows: the import flow adds records the portal does not have yet, the update flow changes existing ones. Both share the following.

- Adds: publications, arXiv preprints, people (authors and reviewers), journals.
- Finds existing items by: DE Number for publications, arXiv ID for arXiv records, zbMATH author ID for people, label plus instance of for journals.
- Rules it follows:
  - The publication date is the year only (see [Publication](#publication)). zbMATH's datestamp is the day zbMATH processed the record, so it is not used.
  - Only reviews become review text; editorials are not stored, and neither is a review text that only points to another record's review ("See the preview in Zbl 0708.49001."). In the update flow, a review that turns into such a pointer is not a change, and the reviewer stays as it is.
  - Authors without a zbMATH author code are author name strings.
  - When zbMATH shows its license-conflict placeholder instead of a title or journal, the title or journal is taken from Crossref through the DOI, if possible.
  - Portal errors are retried with exponential backoff (2 minutes, doubling, at most 1 hour apart, up to 10 hours), because the portal is sometimes unresponsive under bot traffic.

#### Import flow

- Runs: Prefect flow `zbmath-import`, deployment `production`, started by hand. No schedule. One run at a time. Data and checkpoint live on the `importer-workflow-data` volume, and an interrupted run resumes where it stopped.
- Still started regularly: Yes
- Covers: every zbMATH Open record after the highest DE number already downloaded.
- Steps: download the dump, convert it, split it into arXiv and non-arXiv records, drop arXiv records already imported (they are downloaded because they get the highest de numbers), push non-arXiv, push arXiv, add *cites work* for non-arXiv, then for arXiv.

#### Update flow

- Runs: Prefect flow `zbmath-update`, deployment `production`, started by hand. No schedule. One run at a time, with its own volume `importer-update-data`.
- Still started regularly: Not yet
- Covers: records whose content changed between the previous full dump on the volume and a fresh one. New records are left to the import flow.
- Adds and removes: for each changed field, it removes the old value only if the old dump had it and the new one does not, and adds the new value. It moves an author from author name string to *author* once the author gets a zbMATH code. On arXiv items it also updates the arXiv DOI and the author name strings.
- Rules it follows: only values that came from zbMATH are removed, so values added by hand or taken from Crossref survive an update. Titles and review texts are compared by their words only, so a line break that is now a space does not count as a change. The label of an existing person is never changed, because the same person is often named differently in different papers ("Niblo, G. A." in one, "Niblo, Graham A." in the next).
- Never: deletes an item. Records that disappeared from zbMATH are listed in a file on the volume for a later decision.
- Safety stop: if more than half of the records that had a value in a field would lose it (and at least 1,000 records), the run stops before writing, because this might be cause by an error in the zbMath API. Rerun with `allow_mass_deletions=True` once you have checked.
- Trying it out: `max_records_per_pass=20` applies 20 records per pass and stops; `0` stops right after the diff.
- Missing items: a changed record whose item is not in the portal goes to a `missing_in_portal_*` file, and the run is marked failed at the end.

### CRAN-Importer

- Runs: Prefect deployment `cran-nightly`, every day at 02:00 Europe/Berlin, one run at a time.
- Covers: packages published on CRAN since yesterday, then the archive status of every package, then the CRAN task views.
- Adds: R packages, their people, minimal R package items for dependencies not yet in the portal, task view items, and cited publications: it calls the Crossref-, arXiv- and Zenodo-Importer to create them, so they are written under those importers' accounts, and links them with cites work.
- Finds existing items by: label plus instance of R package for packages. People in this order: the item the package already links, a verified ORCID, the same person on other CRAN packages (joined by ORCID or e-mail), the same name with a shared co-author.
- Rules it follows:
  - Licenses, dependencies, imports and cited works are replaced on every update, so they always match the current CRAN page.
  - People are only added. A link someone corrected by hand stays; a maintainer link is removed only when CRAN names another maintainer or ORPHANED.
  - An ORCID is used only when the name registered for it agrees, because DESCRIPTION files sometimes carry someone else's ORCID.
  - A package that leaves CRAN keeps its item and gets an end time on *CRAN project*. If CRAN's index lists fewer than 10,000 packages, the run assumes a broken download and marks nothing as archived.
- Never: relabels a person, adds aliases to a person, or writes e-mail addresses.

### Zenodo-Importer

- Runs: Prefect deployment `zenodo-nightly`, every day at 03:00 Europe/Berlin, one run at a time.
- Covers: records of the Zenodo communities mathplus, mardigmci and mardi updated since yesterday, and any other Zenodo record one of them lists as a related work.
- Adds: one item per Zenodo concept, communities, MATH+ project items, and the works a record lists as related (see the Zenodo resource table for the relations). A missing arXiv preprint is created through the arXiv-Importer, another DOI through the Crossref-Importer, and another Zenodo record through the Zenodo-Importer itself, without that record's own related works.
- Finds existing items by: concept DOI, any version DOI or any Zenodo record ID. When several items match, it prefers one that carries a Zenodo ID, then the oldest. People by ORCID. An author without an ORCID is linked to a person item only if that person is already an author of this item under the same name (same first and last name, or an initial and the same last name); otherwise the author is an author name string.
- Rules it follows:
  - One item per Zenodo concept, so all versions of a dataset or software stay together.
  - Authors and the links to related works (described by source, cites work, based on, part of, has part(s)) are replaced on update, so they match Zenodo. Links are only removed when every related work of the record could be looked up; if one could not, the existing links stay.
  - When the description with the date appended still clashes with another item, the record is not imported and the run is marked failed, so the importer never writes into someone else's item.
- Never: updates an item that has no Zenodo ID. An item someone else created with the same DOI is left alone.

### Research-Theme-Updater

- Runs: Prefect deployment `topic-overviews` from the repository `mardiportal_research_theme_updater`, every day at 02:00 Europe/Berlin.
- Covers: every research theme (instance of `Q7266523`). For each theme, papers from zbMATH Open, OpenAlex and the arXiv search API that match the theme's queries and appeared in the last 10 days, or in the theme's own search window.
- Adds: items for the papers that an LLM puts into a theme, or whose title or abstract contains one of the theme's keywords. An OpenAlex record with a journal, or with a DOI other than an arXiv DOI, is the published article even if it also lists an arXiv version: it becomes a publication item, linked from its arXiv item with *preprint of* when that item is in the portal. Any other record with an arXiv ID becomes an arXiv preprint item, and the rest become publication items. Each gets a TL;DR written by the LLM, with *generated by* pointing to the model's item, and keywords. It also adds *has part(s)* from the theme to the paper, *preprint of* links, and *cites work* links to papers already in the portal.
- Finds existing items by: arXiv ID or arXiv DOI for arXiv records. For other records, zbMATH document ID or DE Number, then DOI and OpenAlex ID, skipping arXiv items. For Zenodo DOIs, also the exact label of a scholarly article. People by zbMATH author ID, or by the ORCID that OpenAlex gives for that author; nobody is matched by name. Journals by label plus instance of scientific journal. Cited works by zbMATH document ID, DOI or OpenAlex ID.
- Rules it follows:
  - The TL;DR is written last. A paper that has one counts as fully imported, and later runs only add identifiers it is missing.
  - Title and publication date are only written when the item has none. A publication date from zbMATH is the year only.
  - An author is either linked to a person item, found by identifier, or written as an author name string, never both. When an author is linked, name strings on the item that match the author's name are removed. If the portal cannot be reached during the lookup, neither is written and the paper is completed on the next run.
  - A new item for a zbMATH record gets the zbMATH-Importer's description, and a new arXiv preprint item gets "scientific article from arXiv", like the arXiv-Importer writes. Other new publication items, such as those from OpenAlex, get no description. If that label and description are already taken, the arXiv or zbMATH ID is added in brackets.
  - License URLs are mapped to license items through a fixed table; a URL that is not in the table is left out.
  - When Semantic Scholar gives a cited work without an identifier, the citation is matched by its exact title: only to a publication, only when exactly one publication has that title, and only for titles of four words or more. These links carry the reference *stated in* Semantic Scholar.
  - A theme is trimmed to 100 *has part(s)* links. Above that, duplicate links are removed first, then the papers with the oldest publication date, including papers someone added by hand. Entries without a publication date, such as the datasets linked so far, are never removed except as duplicates, even if that leaves the theme above 100. They still count towards the 100, so each one leaves room for one paper fewer.
- Never: writes zbMATH IDs or a publisher DOI onto arXiv items, or arXiv IDs onto zbMATH items. Creates no person, journal or cited-work items; it only links existing ones.
- Known problems:
  - For zbMATH records of arXiv preprints, the updater fetches more data from OpenAlex. If OpenAlex has merged the preprint and the journal article into one work, the arXiv item gets the publication date, open access status, concepts and keywords of the published article.
  - When zbMATH has no references for a paper from the arXiv search, the citation step asks Crossref with the journal DOI whenever arXiv lists one. The arXiv item then gets the journal article's references as *cites work* and its license.
  - An arXiv item whose published version is in zbMATH gets the published article's zbMATH reference list as *cites work*.
  - Keywords from the LLM and keywords from OpenAlex both go into keywords (P1964) and cannot be told apart.
  - Items written by earlier versions can still have arXiv IDs on published items, publisher DOIs or the journal's data on arXiv items, authors listed both as person items and as name strings, "Last, First" name strings, and authors or citations matched by name or title.

### Julia-Importer

- Runs: CLI `mardi-importer import-julia`, by hand. `--dry-run` reports without writing; `--packages` limits the run.
- Still started regularly: unclear
- Covers: about 290 packages from the Julia General registry under SciML, JuliaMath, JuliaLinearAlgebra, JuliaNLSolvers and JuliaDiff. No `_jll` wrappers, no sub-directory packages except StochasticDiffEq and DelayDiffEq.
- Adds: Julia packages, people, cited publications: when a cited DOI or arXiv ID is not in the portal yet, it calls the Crossref- or arXiv-Importer to create it, so it is written under that importer's account, and links it with described by source.
- Finds existing items by: registry package name, the package's own repository, or the package name together with a repository named `<Name>.jl`.
- Rules it follows: an existing item is touched only when it is certainly the same software, because many names are shared (SUNDIALS and Sundials.jl). A person item is reused only with an ORCID or as the same-named author of a paper the package cites.
- Never: overwrites or removes anything. A different existing value on a single-valued property is logged as a conflict. Zenodo DOIs in citation files are the software's own releases and are not linked as publications.

### OpenML-Importer

One importer with two Prefect flows, built like the zbMATH-Importer: the import flow adds datasets the portal does not have yet, the update flow changes existing ones. Both share the following.

- Covers: every active dataset listed by the OpenML API (`/api/v1/json/data/list`).
- Adds: OpenML datasets, and the papers they cite (through the Crossref- or arXiv-Importer).
- Finds existing items by: OpenML dataset ID for datasets; DOI or arXiv ID for papers.
- Rules it follows:
  - Both flows start by downloading every dataset's record as one line in `openml_<date>.jsonl`. An interrupted download continues where it stopped.
  - OpenML gives people as names only, so creators and contributors are author name strings.
  - Portal errors are retried with exponential backoff, as in the zbMATH-Importer.
  - A dataset that still fails is counted, and the run is marked failed when that step ends. The run carries on with the next datasets, and the failed IDs are saved in the checkpoint, so the next run retries only those and does not redo the datasets after them; the update flow also keeps its comparison baseline until then.
- Older code: the files of the earlier OpenML importer that are no longer used are kept in `archive/openml/`, outside the package.

#### Import flow

- Runs: Prefect flow `openml-import`, deployment `production`, started by hand. No schedule. One run at a time. Data and checkpoint live on its own volume `importer-openml-data` (mounted at `/mnt/openml-data`), and an interrupted run resumes where it stopped.
- Still started regularly: Not yet
- Steps: download, then create an item for every dataset whose OpenML dataset ID is not in the portal. Datasets already in the portal are left to the update flow.
- Trying it out: `max_records=20` creates 20 items and stops; the next run continues after them.

#### Update flow

- Runs: Prefect flow `openml-update`, deployment `production`, started by hand. No schedule. One run at a time, with its own volume `importer-openml-update-data` (mounted at `/mnt/openml-update-data`). The OpenML flows share no volume with the zbMATH flows, because a volume can be attached to only one pod at a time.
- Still started regularly: Not yet
- Covers: datasets whose record changed between the previous download on the volume and a fresh one. New datasets are left to the import flow.
- Adds and removes: a value is removed only if the previous download had it and the new one does not; values added by hand stay. An item has one OpenML download link: once the new record has one, any other OpenML download link on the item (an older host such as api.openml.org, or a replaced file) is removed. A label is changed only if it still is the old OpenML name.
- First run: without a previous download every dataset is checked. Nothing is removed then, except values the earlier importer wrote in an outdated form: the profile item "Dataset", descriptions with literal `\N` and `\T` codes, and OpenML download links on an older host, which are replaced by the current form.
- Never: deletes an item. Datasets that disappeared from OpenML are listed in `openml_<date>_gone_from_openml.txt`.
- Safety stop: if more than half of the datasets that had a value in a field would lose it (and at least 100), the run stops before writing. Rerun with `allow_mass_deletions=True` once you have checked.
- Missing items: a changed dataset whose item is not in the portal goes to `openml_<date>_missing_in_portal.txt`, and the run is marked failed at the end.
- Trying it out: `max_records_per_pass=20` checks 20 changed datasets and stops.

### Crossref-Importer and arXiv-Importer

- Runs: only when another importer needs a publication item, mainly the CRAN-, Julia- and Zenodo-Importer.
- Adds: publications, arXiv preprints, journals, books, proceedings, people.
- Finds existing items by: DOI in upper case, Crossref's "is identical to" DOI, ISBN for books, arXiv ID, and ORCID for people.
- Rules it follows: an author becomes a person item only with an ORCID; everyone else is an author name string, so no person items are created on a name alone.

### Wikidata-Importer

- Runs: on request through the API or CLI `import-wikidata`. Also once per container when an importer first starts, for the entities in its `wikidata_entities.txt`.
- Still started regularly: on request
- Adds: a copy of the Wikidata item or property with labels, descriptions and aliases in en, de and mul, all its statements, and *Wikidata QID* or *Wikidata PID*. Items and properties that the statements point to are copied with labels only.
- Finds existing items by: the mapping tables `items` and `properties` in the importer database (Wikidata ID to local ID). If the mapped item was deleted in the portal, the mapping is dropped and the entity is imported again.
- Rules it follows: every copy records its Wikidata ID and the mapping table is kept up to date, so code can write `wdt:P50` or `wd:Q5`, and mardiclient looks up the local ID through the mapping service.
- Never: copies lexemes or the properties listed in `excluded_properties`.

### polyDB-Importer

- Runs: `scripts/import.py --mode polydb`, by hand.
- Still started regularly: No longer
- Covers: 21 collections listed in `PolyDBSource.py`, with 28 authors whose ORCIDs, affiliations and Wikidata QIDs are written into the code.
- Adds: polyDB collections, their authors, the publications they cite (through its own older copy of the Crossref and arXiv code), and items for the 2007 Kyoto RIMS workshop that one collection comes from.

### Item API

- Runs: on request, `POST /create/item`, `POST /update/item`, CLI `create-item`. Every call uses the caller's own bot account; there is no shared fallback account, so every edit is attributed to the person who made it.
- Adds: any item (raw format with local property and item IDs) or a typed item. The only type so far is WORKFLOW, defined in `services/item_schemas.py`.
- Never: overwrites existing values on `/update/item` unless `do_override` is set; without it the call is refused with the values that are there.

### Correction scripts (`bot_scripts/`)

One-off scripts, run by hand in the importer pod with their own bot accounts: removing references to deleted items (`delete_references.py`; should be checked for correctness before use), rerouting statements from merged items to the surviving item (`reroute.py`; should be checked for correctness before use), and removing the *object named as* qualifier from Julia authors (`julia_remove_author_name_qualifiers.py`).

## Writing a new bot

### Checklist

- Find the item type you write in the tables above and use the same instance of, the same properties and the same MaRDI profile type.
- Before creating an item, check whether it already exists. Search by the same identifier the existing importers use for that kind of item: the DOI for general publications, the arXiv ID for preprints, the zbMATH DE Number for zbMATH publications, the ORCID or zbMATH author ID for people, the Zenodo ID or DOI for Zenodo records. Each importer's "Finds existing items by" line lists them. Never match people or software on the label alone.
- Keep arXiv preprints and published versions apart; link them with *preprint of* (P1676).
- Write Wikidata properties and classes as `wdt:P…` and `wd:Q…` and let mardiclient translate them. If one is not in the portal yet, add it to your source's `wikidata_entities.txt`.
- Declare portal-only properties and items in `new_entities.json` and refer to them by their exact English label.
- Store DOIs in upper case. Write dates as `+YYYY-MM-DDT00:00:00Z` with precision 9 (year), 10 (month) or 11 (day); a day-precision time must be at midnight.
- Create a person item only with an ORCID or another identifier; otherwise write an author name string.
- Remove a value only when the task says to remove it. Adding a value must never replace the others, unless there is a good reason; check the defaults of every call that writes statements.
- Retry portal errors with backoff, and cache items created in the same run, because it can take a while for them to show up in searches

### Schema changes

A new bot sometimes needs something the schema does not have yet: a new property, a new class, a new profile type, or an existing property used in a new way. That can be the right call, as long as someone decides it on purpose.

If an AI coding assistant writes the bot from this document: when the bot is finished, end your summary with a section called "Schema changes" that lists every change the bot makes to the schema:

- new properties and items it creates, with label, datatype, and whether Wikidata already has an equivalent
- existing properties it uses on a new kind of item or with a different meaning
- item types whose instance of, label or description differ from what this document describes
- every value it removes or replaces, with the part of the task that asks for it, and any items it merges

If there are none, say so. For each change, give the reason in one sentence and ask the person to check whether it fits the existing schema before the bot runs against the portal.

If you are the person: read that list before the first run. Dropping a schema change is easy before the bot has written its items and tedious afterwards.
