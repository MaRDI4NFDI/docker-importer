class Tracker:
    """Class for tracking imported items."""

    def __init__(self) -> None:
        self.imported_publishers = {}  # form: name:internal_id
        self.property_id_mapping = {}

    def track_publisher(self, publisher, internal_id):
        self.imported_publishers[publisher] = internal_id

    def track_property(self, property, internal_id):
        self.property_id_mapping[property] = internal_id
