"""Type schemas for creating pre-filled Wikibase items.

Each schema defines:
  - predefined_claims: list of (pid, value) tuples always written on the new item.
    A list (not a dict) is used so the same property can appear more than once.
  - fields: mapping of human-readable field names to their target.

Field targets:
  "label"       — sets the item label (one per schema, required)
  "description" — sets the item description
  "claim"       — adds a top-level claim; requires a "pid" key
  "qualifier"   — attaches as a qualifier on another field's claim;
                  requires "pid" and "qualifier_for" (the field name of the parent claim)

resolve_typed_item() returns claims as a list of dicts:
  {"pid": str, "value": str, "qualifiers": [{"pid": str, "value": str}, ...]}
so that duplicate PIDs and qualifier relationships are preserved.
"""

from __future__ import annotations

SCHEMAS: dict[str, dict] = {
    "WORKFLOW": {
        # P31 = instance of: research workflow (Q68657) + script-based workflow (Q6830884)
        # P1460 = MaRDI profile type: MaRDI workflow profile (Q6534216)
        "predefined_claims": [
            ("P31", "Q68657"),
            ("P31", "Q6830884"),
            ("P1460", "Q6534216"),
        ],
        "fields": {
            "name": {
                "target": "label",
                "required": True,
            },
            "problem_statement": {
                "target": "claim",
                "pid": "P1604",
                "required": True,
            },
            # P557 — uses: QID of a dataset or resource the workflow depends on
            "uses": {
                "target": "claim",
                "pid": "P557",
                "required": False,
            },
            # P16 — author: QID of a person
            "author": {
                "target": "claim",
                "pid": "P16",
                "required": False,
            },
            # P28 — publication date: YYYY-MM-DD, converted to Wikibase time format
            "publication_date": {
                "target": "claim",
                "pid": "P28",
                "required": False,
                "value_format": "wikibase_time",
            },
            # P163 — copyright license: QID of a license, e.g. CC-BY-SA = Q57031
            "copyright_license": {
                "target": "claim",
                "pid": "P163",
                "required": False,
            },
            # P223 — cites work: QID of a related publication
            "cites_work": {
                "target": "claim",
                "pid": "P223",
                "required": False,
            },
            # P1827 — stored at: QID of storage location, e.g. MaRDI data store = Q6830870
            # Must be accompanied by fdo_component_id (P1828 qualifier).
            "stored_at": {
                "target": "claim",
                "pid": "P1827",
                "required": False,
                "requires": ["fdo_component_id"],
            },
            # P1828 — FDO component id: qualifier on the stored_at claim, e.g. "rocrate.zip"
            # If stored_at is not provided, defaults to Q6830870 (MaRDI data store).
            "fdo_component_id": {
                "target": "qualifier",
                "qualifier_for": "stored_at",
                "pid": "P1828",
                "required": False,
                "default_parent_value": "Q6830870",
            },
        },
    },
}

KNOWN_TYPES: frozenset[str] = frozenset(SCHEMAS)


def _to_wikibase_time(value: str) -> str:
    """Convert a YYYY-MM-DD date string to Wikibase time format (+YYYY-MM-DDT00:00:00Z)."""
    import re
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return f"+{value}T00:00:00Z"
    return value


def resolve_typed_item(
    type_name: str, fields: dict
) -> tuple[str | None, str | None, list[dict], list[str]]:
    """Map a typed request to label, description, and a structured claims list.

    Each claim in the returned list is a dict:
        {"pid": str, "value": str, "qualifiers": [{"pid": str, "value": str}, ...]}

    Qualifiers are attached to their parent claim rather than emitted as
    separate top-level statements.

    Args:
        type_name: Schema key, e.g. "WORKFLOW".
        fields: Human-readable key/value pairs from the caller.

    Returns:
        Tuple of (label, description, claims, errors). ``errors`` is non-empty
        when required fields are missing or the type is unknown.
    """
    schema = SCHEMAS.get(type_name)
    if schema is None:
        return None, None, [], [f"Unknown type '{type_name}'. Known types: {sorted(KNOWN_TYPES)}"]

    errors: list[str] = []
    for key, field_def in schema["fields"].items():
        if field_def["required"] and key not in fields:
            errors.append(f"Missing required field: '{key}'")
        if key in fields:
            for dep in field_def.get("requires", []):
                if dep not in fields:
                    errors.append(f"Field '{key}' requires '{dep}' to also be set.")
    if errors:
        return None, None, [], errors

    label: str | None = None
    description: str | None = None

    # Predefined claims have no qualifiers.
    claims: list[dict] = [
        {"pid": pid, "value": value, "qualifiers": []}
        for pid, value in schema.get("predefined_claims", [])
    ]
    # Index user-provided claim dicts by field name for qualifier lookup.
    claim_by_field: dict[str, dict] = {}

    # First pass: label, description, and top-level claims.
    for key, value in fields.items():
        field_def = schema["fields"].get(key)
        if field_def is None:
            continue
        target = field_def["target"]
        if target == "label":
            label = value
        elif target == "description":
            description = value
        elif target == "claim":
            if field_def.get("value_format") == "wikibase_time":
                value = _to_wikibase_time(value)
            claim_dict: dict = {"pid": field_def["pid"], "value": value, "qualifiers": []}
            claim_by_field[key] = claim_dict
            claims.append(claim_dict)

    # Second pass: attach qualifiers to their parent claims.
    # If the parent claim was not supplied, fall back to default_parent_value when defined.
    for key, value in fields.items():
        field_def = schema["fields"].get(key)
        if field_def is None or field_def["target"] != "qualifier":
            continue
        parent_key = field_def["qualifier_for"]
        parent = claim_by_field.get(parent_key)
        if parent is None:
            default_value = field_def.get("default_parent_value")
            if default_value is not None:
                parent_pid = schema["fields"][parent_key]["pid"]
                parent = {"pid": parent_pid, "value": default_value, "qualifiers": []}
                claim_by_field[parent_key] = parent
                claims.append(parent)
        if parent is not None:
            parent["qualifiers"].append({"pid": field_def["pid"], "value": value})

    if not label:
        return None, None, [], ["No label field defined in schema or value is empty."]

    return label, description, claims, []
