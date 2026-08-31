"""Importer for the MIPLIB 2017 mixed-integer programming library.

MaRDI already holds the collection itself as ``Q6826034`` -- *MIPLIB 2017
benchmark set for mixed-integer programming*, imported from MathAlgoDB. What is
missing is its membership: the item carries five statements and nothing points
at it. This source creates one item per instance and links each back to the
collection with *part of*.

MIPLIB publishes no CSV or JSON export, so instance statistics are parsed out of
the per-instance HTML pages. The roster itself is a plain-text list, one
``<name>.mps.gz`` per line.
"""

import time

import requests
from bs4 import BeautifulSoup

from mardi_importer.base import ADataSource
from mardi_importer.logger import get_logger_safe

from .MIPLIBInstance import MIPLIBInstance

log = get_logger_safe(__name__)

BASE_URL = "https://miplib.zib.de"
COLLECTION_PATH = "/downloads/collection-v1.test"
INSTANCE_PATH = "/instance_details_{}.html"

SUMMARY_FIELDS = {
    "Submitter": "submitter",
    "Status": "status",
    "Group": "group",
    "Objective": "objective",
    "Density": "density",
    "MPS File": "mps_file",
}

#: Only the *Original* column is imported; presolved figures are a property of
#: the presolver rather than of the instance.
STATISTICS_FIELDS = {
    "Variables": "num_variables",
    "Constraints": "num_constraints",
    "Binaries": "num_binaries",
    "Integers": "num_integers",
    "Continuous": "num_continuous",
    "Implicit Integers": "num_implicit_integers",
    "Fixed Variables": "num_fixed_variables",
    "Nonzeroes": "num_nonzeros",
}


class MIPLIBSource(ADataSource):
    """Reads instance metadata from the MIPLIB 2017 website."""

    def __init__(
        self,
        user: str,
        password: str,
        instance_limit: int = None,
        instance_names: list = None,
        request_delay: float = 0.5,
    ):
        super().__init__(user, password)
        self.instances = []
        self.instance_limit = instance_limit
        self.instance_names = instance_names
        self.request_delay = request_delay

    def setup(self):
        """Create all necessary properties and entities for MIPLIB."""
        self.import_wikidata_entities("/wikidata_entities.txt")
        self.create_local_entities("/new_entities.json")
        self.set_formatter_url()

    def set_formatter_url(self):
        """Give the instance identifier property a formatter URL.

        ``create_local_entities`` only sets label, description and datatype, so
        the formatter URL has to be added separately. With it in place the
        identifier renders as a link and no per-instance URL need be stored.
        """
        url = BASE_URL + INSTANCE_PATH.format("$1")
        try:
            prop_nr = self.api.get_local_id_by_label(
                "MIPLIB instance ID", "property"
            )
            if isinstance(prop_nr, list):
                prop_nr = prop_nr[0]
            prop = self.api.property.get(entity_id=prop_nr)
            if prop.claims.get("wdt:P1630"):
                return
            prop.claims.add(self.api.get_claim("wdt:P1630", url))
            prop.write()
            log.info("Formatter URL set on %s", prop_nr)
        except Exception as exc:
            log.warning("Could not set the formatter URL: %s", exc)

    def pull(self):
        """Fetch the instance roster and the statistics of each instance.

        ``instance_names`` restricts the run to named instances and skips the
        roster entirely; ``instance_limit`` truncates it. Both exist so that a
        trial run can import a handful of instances before the full collection.
        """
        if self.instance_names:
            names = list(self.instance_names)
        else:
            names = self.fetch_collection()
        if self.instance_limit:
            names = names[: self.instance_limit]
        log.info("Pulling %d MIPLIB instances", len(names))

        self.instances = []
        for name in names:
            record = self.fetch_instance(name)
            if record:
                self.instances.append(record)
            time.sleep(self.request_delay)

        log.info("Parsed %d MIPLIB instances", len(self.instances))
        return self.instances

    def push(self):
        """Create or update a MaRDI item for each instance."""
        if not self.instances:
            log.warning("Nothing to push; run pull() first")
            return

        known = self.existing_qids([r["name"] for r in self.instances])

        for record in self.instances:
            instance = MIPLIBInstance(
                api=self.api, QID=known.get(record["name"]), **record
            )
            if not instance.exists():
                instance.create()
            else:
                instance.update()

    def existing_qids(self, names):
        """Resolve instance names to QIDs in a single query.

        One batched lookup keeps ``push`` to a single SPARQL round trip rather
        than one per instance.
        """
        try:
            prop_nr = self.api.get_local_id_by_label(
                "MIPLIB instance ID", "property"
            )
            if isinstance(prop_nr, list):
                prop_nr = prop_nr[0]
            results = self.api.batch_search_by_value(prop_nr, names)
        except Exception as exc:
            log.warning("Batch lookup failed, falling back per item: %s", exc)
            return {}
        return {name: qids[0] for name, qids in results.items() if qids}

    def new_instance(self, miplib_id: str) -> "MIPLIBInstance":
        """Build a single instance on demand, without a full pull."""
        record = self.fetch_instance(miplib_id)
        if not record:
            return None
        return MIPLIBInstance(api=self.api, **record)

    def fetch_collection(self):
        """Return the names of every instance in the MIPLIB collection."""
        text = self.get_with_retries(BASE_URL + COLLECTION_PATH)
        if text is None:
            raise RuntimeError(
                "MIPLIB collection list could not be retrieved; refusing to "
                "continue with an empty roster"
            )
        return self.parse_collection(text)

    def fetch_instance(self, name):
        """Return the parsed record for one instance, or ``None``."""
        try:
            html = self.get_with_retries(
                BASE_URL + INSTANCE_PATH.format(name)
            )
        except requests.exceptions.RequestException as exc:
            log.warning("Could not fetch instance %s: %s", name, exc)
            return None
        if html is None:
            log.warning("Gave up fetching instance %s after retries", name)
            return None
        return self.parse_instance(html, name)

    @staticmethod
    def get_with_retries(url, retries=5, base_wait=3):
        """Fetch a URL, backing off on rate limits and transient failures."""
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=30)

                if response.status_code == 429:
                    time.sleep(base_wait * (2**attempt))
                    continue

                response.raise_for_status()
                return response.text

            except requests.exceptions.HTTPError:
                if 400 <= response.status_code < 500:
                    raise

            except requests.exceptions.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(base_wait * (2**attempt))

    @staticmethod
    def parse_collection(text):
        """Extract instance names from a MIPLIB ``.test`` list.

        Each line is a file name such as ``30n20b8.mps.gz``; the instance name
        is that with the extension removed.
        """
        names = []
        for line in (text or "").splitlines():
            line = line.strip()
            if not line:
                continue
            names.append(line.split(".mps")[0])
        return names

    @staticmethod
    def parse_instance(html, name):
        """Parse one instance detail page.

        The page carries the instance name in an ``h1``, its tags as labelled
        links in the ``h3`` directly below, a summary table, and a table of
        Original/Presolved statistics.
        """
        soup = BeautifulSoup(html, "lxml")
        record = {"name": name, "tags": []}

        heading = soup.find("h1")
        if heading:
            tag_block = heading.find_next_sibling("h3")
            if tag_block:
                record["tags"] = [
                    link.get_text(strip=True)
                    for link in tag_block.find_all("a")
                    if link.get_text(strip=True)
                ]

        tables = soup.find_all("table")

        if tables:
            summary = MIPLIBSource._parse_summary(tables[0])
            for heading_text, key in SUMMARY_FIELDS.items():
                record[key] = summary.get(heading_text)

        if len(tables) > 1:
            statistics = MIPLIBSource._parse_statistics(tables[1])
            for row_label, key in STATISTICS_FIELDS.items():
                record[key] = statistics.get(row_label)

        # The difficulty status is also a MIPLIB tag; keep the tag list complete
        # so a single property carries every membership signal.
        status = record.get("status")
        if status and status not in record["tags"]:
            record["tags"].append(status)

        return record

    @staticmethod
    def _parse_summary(table):
        """Read the two-row summary table into a heading -> value mapping."""
        rows = table.find_all("tr")
        if len(rows) < 2:
            return {}
        headings = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        values = [c.get_text(strip=True) for c in rows[1].find_all(["th", "td"])]
        return dict(zip(headings, values))

    @staticmethod
    def _parse_statistics(table):
        """Read the Original column of the statistics table.

        Rows carry a label followed by the original and presolved figures; the
        header row has only two cells and is skipped by the length check.
        """
        statistics = {}
        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
            if len(cells) >= 3 and cells[0]:
                statistics[cells[0]] = cells[1]
        return statistics
