"""Transform raw MySQL tag prompt tables into domain objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger

from .mysql_importer import DEFAULT_TAG_PROMPT_TABLES


_TAGGER_KEYS = ("tagger_id", "worker_id", "user_id")
_COMMENT_KEYS = ("comment_id", "item_id", "document_id")
_CHARACTERISTIC_KEYS = (
    "characteristic_id",
    "tag_prompt_deployment_characteristic_id",
    "characteristic_slug",
)
_VALUE_KEYS = ("value", "response", "answer", "label")
_TIMESTAMP_KEYS = (
    "tagged_at",
    "created_at",
    "updated_at",
    "completed_at",
    "timestamp",
)


@dataclass(frozen=True)
class TagPromptDeploymentDataset:
    """Collection of domain objects derived from tag prompt deployment tables."""

    assignments: List[TagAssignment]
    taggers: List[Tagger]

    @classmethod
    def from_mysql_tables(
        cls,
        tables: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        answers_table: Optional[str] = None,
    ) -> "TagPromptDeploymentDataset":
        """Create a dataset from the raw tables fetched via :mod:`mysql_importer`.

        Parameters
        ----------
        tables:
            Mapping of table name to an iterable of row dictionaries. This is the
            structure returned by :func:`import_tag_prompt_deployment_tables`.
        answers_table:
            Optional override for which table contains the assignment rows. When
            omitted, the first table in :data:`DEFAULT_TAG_PROMPT_TABLES` that is
            present in *tables* is used.
        """

        if not tables:
            return cls(assignments=[], taggers=[])

        source_table = _select_answers_table(tables, answers_table)
        rows = tables.get(source_table, [])

        assignments = []
        for row in rows:
            assignment = _row_to_assignment(row)
            if assignment is not None:
                assignments.append(assignment)

        taggers = _group_assignments_by_tagger(assignments)
        return cls(assignments=assignments, taggers=taggers)

    def as_domain_dict(self) -> Dict[str, Any]:
        """Return a mapping resembling the CSV adapter contract."""

        return {
            "assignments": self.assignments,
            "taggers": self.taggers,
            "comments": [],
            "prompts": [],
            "characteristics": [],
        }


def _select_answers_table(
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    answers_table: Optional[str],
) -> str:
    if answers_table and answers_table in tables:
        return answers_table

    for name in DEFAULT_TAG_PROMPT_TABLES:
        if name in tables:
            return name

    # Fall back to deterministic selection for predictable behaviour in tests.
    return next(iter(tables.keys()))


def _row_to_assignment(row: Mapping[str, Any]) -> Optional[TagAssignment]:
    try:
        tagger_id = _extract_first(row, _TAGGER_KEYS)
        comment_id = _extract_first(row, _COMMENT_KEYS)
        characteristic_id = _extract_first(row, _CHARACTERISTIC_KEYS)
        value_raw = _extract_first(row, _VALUE_KEYS)
        timestamp_raw = _extract_first(row, _TIMESTAMP_KEYS)
    except KeyError:
        return None

    if not all([tagger_id, comment_id, characteristic_id, value_raw, timestamp_raw]):
        return None

    try:
        tag_value = _parse_tag_value(value_raw)
        timestamp = _parse_timestamp(timestamp_raw)
    except ValueError:
        return None

    return TagAssignment(
        tagger_id=str(tagger_id),
        comment_id=str(comment_id),
        characteristic_id=str(characteristic_id),
        value=tag_value,
        timestamp=timestamp,
    )


def _extract_first(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    raise KeyError(keys)


def _parse_tag_value(value: Any) -> TagValue:
    if isinstance(value, TagValue):
        return value

    if value is None:
        raise ValueError("Tag value is required")

    value_str = str(value).strip()
    if not value_str:
        raise ValueError("Tag value cannot be empty")

    normalized = value_str.upper()

    alias_map = {
        "Y": TagValue.YES,
        "YES": TagValue.YES,
        "TRUE": TagValue.YES,
        "T": TagValue.YES,
        "N": TagValue.NO,
        "NO": TagValue.NO,
        "FALSE": TagValue.NO,
        "F": TagValue.NO,
        "NA": TagValue.NA,
        "N/A": TagValue.NA,
        "UNKNOWN": TagValue.NA,
        "UNCERTAIN": TagValue.UNCERTAIN,
        "SKIP": TagValue.SKIP,
    }

    if normalized in alias_map:
        return alias_map[normalized]

    try:
        return TagValue(normalized)
    except ValueError:
        pass

    try:
        return TagValue[value_str]
    except KeyError as exc:
        raise ValueError(f"Unrecognised tag value: {value_str!r}") from exc


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if value is None:
        raise ValueError("Timestamp is required")

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    value_str = str(value).strip()
    if not value_str:
        raise ValueError("Timestamp cannot be empty")

    iso_candidate = value_str.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value_str!r}") from exc

    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _group_assignments_by_tagger(assignments: Sequence[TagAssignment]) -> List[Tagger]:
    grouped: MutableMapping[str, List[TagAssignment]] = {}
    for assignment in assignments:
        grouped.setdefault(assignment.tagger_id, []).append(assignment)

    taggers: List[Tagger] = []
    for tagger_id in sorted(grouped.keys()):
        tag_assignments = sorted(grouped[tagger_id], key=lambda a: a.timestamp)
        taggers.append(Tagger(id=tagger_id, tagassignments=tag_assignments))
    return taggers
