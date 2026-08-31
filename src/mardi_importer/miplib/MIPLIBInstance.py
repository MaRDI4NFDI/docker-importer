"""A single problem instance from the MIPLIB 2017 library."""

from mardi_importer.logger import get_logger_safe

log = get_logger_safe(__name__)

#: The MIPLIB 2017 benchmark set, already present in MaRDI via MathAlgoDB.
COLLECTION_QID = "Q6826034"

#: *MIPLIB 2017: data-driven compilation of the 6th mixed-integer programming
#: library*. The zbMATH-derived item is used in preference to ``Q6824324``,
#: which is a thinner MathAlgoDB-derived duplicate of the same DOI.
PAPER_QID = "Q2062317"

QUANTITY_PROPERTIES = {
    "num_variables": "number of variables",
    "num_constraints": "number of constraints",
    "num_binaries": "number of binary variables",
    "num_integers": "number of integer variables",
    "num_continuous": "number of continuous variables",
    "num_implicit_integers": "number of implicit integer variables",
    "num_fixed_variables": "number of fixed variables",
    "num_nonzeros": "number of nonzeros",
}

#: Values MIPLIB uses for a field it has no figure for.
MISSING = {"", "-", "–", "—", "None", "n/a"}


def to_int(value):
    """Return ``value`` as an int, or ``None`` when MIPLIB records no figure."""
    if value is None or str(value).strip() in MISSING:
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except ValueError:
        return None


def to_float(value):
    """Return ``value`` as a float, or ``None`` when MIPLIB records no figure."""
    if value is None or str(value).strip() in MISSING:
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def clean(value):
    """Return a stripped string, or ``None`` when MIPLIB records no value."""
    if value is None or str(value).strip() in MISSING:
        return None
    return str(value).strip()


class MIPLIBInstance:
    """Creates and updates the MaRDI item for one MIPLIB instance."""

    def __init__(
        self,
        api,
        name,
        tags=None,
        submitter=None,
        status=None,
        group=None,
        objective=None,
        density=None,
        mps_file=None,
        num_variables=None,
        num_constraints=None,
        num_binaries=None,
        num_integers=None,
        num_continuous=None,
        num_implicit_integers=None,
        num_fixed_variables=None,
        num_nonzeros=None,
        QID=None,
    ):
        self.api = api
        self.name = name
        self.tags = [clean(tag) for tag in (tags or []) if clean(tag)]
        self.submitter = clean(submitter)
        self.status = clean(status)
        self.group = clean(group)
        self.objective = to_float(objective)
        self.density = to_float(density)
        self.mps_file = clean(mps_file)

        self.statistics = {
            key: to_int(value)
            for key, value in {
                "num_variables": num_variables,
                "num_constraints": num_constraints,
                "num_binaries": num_binaries,
                "num_integers": num_integers,
                "num_continuous": num_continuous,
                "num_implicit_integers": num_implicit_integers,
                "num_fixed_variables": num_fixed_variables,
                "num_nonzeros": num_nonzeros,
            }.items()
        }

        self.collection_qid = COLLECTION_QID
        self.paper_qid = PAPER_QID
        self.QID = QID
        self._class_qid = None
        self.item = self.init_item()

    @property
    def class_qid(self):
        """QID of the *MIPLIB instance* item every instance is typed with."""
        if self._class_qid is None:
            qid = self.api.get_local_id_by_label("MIPLIB instance", "item")
            self._class_qid = qid[0] if isinstance(qid, list) else qid
        return self._class_qid

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        item.descriptions.set(
            language="en", value=f"MIPLIB 2017 instance {self.name}"
        )
        return item

    def create(self):
        self.item.add_claim("wdt:P31", self.class_qid)
        self.insert_claims()
        instance_id = self.item.write().id
        log.info("MIPLIB instance %s created with QID %s", self.name, instance_id)
        return instance_id

    def exists(self):
        """Return the QID of this instance if it is already in the graph.

        Keyed on the MIPLIB identifier rather than the label: instance names
        such as ``ex9`` or ``neos-1122047`` are short and collide easily, and a
        label match alone would not establish identity.
        """
        if self.QID:
            return self.QID
        self.QID = self.item.is_instance_of_with_property(
            self.class_qid, "MIPLIB instance ID", self.name
        )
        return self.QID

    def update(self):
        self.item = self.api.item.get(entity_id=self.QID)
        self.insert_claims()
        self.item.write()
        log.info("MIPLIB instance %s updated (%s)", self.name, self.QID)
        return self.QID

    def submitter_names(self):
        """Split the submitter field into individual names.

        Values are usually a single person or an institution ("Jeff Linderoth",
        "MIPLIB submission pool"); a comma separates co-submitters, as in
        "E. Coughlan, M. Lübbecke, J. Schulz". Names are stored as strings
        rather than resolved to person items: MIPLIB abbreviates given names to
        an initial, which is not enough to establish identity.
        """
        if not self.submitter:
            return []
        return [part.strip() for part in self.submitter.split(",") if part.strip()]

    def insert_claims(self):
        prop_nr = self.api.get_local_id_by_label("MIPLIB instance ID", "property")
        self.item.add_claim(prop_nr, self.name)

        self.item.add_claim("wdt:P361", self.collection_qid)
        self.item.add_claim("wdt:P1343", self.paper_qid)

        if self.tags:
            prop_nr = self.api.get_local_id_by_label("MIPLIB tag", "property")
            self.item.add_claims(
                [self.api.get_claim(prop_nr, tag) for tag in sorted(set(self.tags))]
            )

        if self.submitter:
            self.item.add_claims(
                [
                    self.api.get_claim("wdt:P2093", name)
                    for name in self.submitter_names()
                ]
            )

        if self.group:
            prop_nr = self.api.get_local_id_by_label(
                "MIPLIB instance group", "property"
            )
            self.item.add_claim(prop_nr, self.group)

        if self.mps_file:
            prop_nr = self.api.get_local_id_by_label("MPS file name", "property")
            self.item.add_claim(prop_nr, self.mps_file)

        for key, label in QUANTITY_PROPERTIES.items():
            value = self.statistics.get(key)
            if value is not None:
                prop_nr = self.api.get_local_id_by_label(label, "property")
                self.item.add_claim(prop_nr, value)

        if self.density is not None:
            prop_nr = self.api.get_local_id_by_label("nonzero density", "property")
            self.item.add_claim(prop_nr, self.density)

        if self.objective is not None:
            prop_nr = self.api.get_local_id_by_label("objective value", "property")
            self.item.add_claim(prop_nr, self.objective)

        self.item.add_claim("MaRDI profile type", "MaRDI dataset profile")
