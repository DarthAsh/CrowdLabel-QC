"""Database adapter for reading crowd labeling data from MySQL."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, Iterable, List, Mapping, Optional, Sequence

from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.data_ingestion.mysql_importer import DEFAULT_TAG_PROMPT_TABLES, TableImporter
from qcc.domain.characteristic import Characteristic
from qcc.domain.comment import Comment
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.domain.enums import TagValue


class DBAdapter:
    """Adapter that loads QCC domain objects from a MySQL database."""

    def __init__(
        self,
        mysql_config: MySQLConfig,
        *,
        importer: Optional[TableImporter] = None,
        tables: Sequence[str] = DEFAULT_TAG_PROMPT_TABLES,
    ) -> None:
        """Create the adapter.

        Parameters
        ----------
        mysql_config:
            MySQL connection information.
        importer:
            Optional ``TableImporter`` instance. Mostly useful for tests where a
            fake importer can be supplied. When not provided, a ``TableImporter``
            is created using ``mysql_config``.
        tables:
            The database tables that should be queried. The first entry is
            expected to contain the tag assignments.
        """

        if not tables:
            raise ValueError("At least one table must be provided for import")

        self._config = mysql_config
        self._importer = importer or TableImporter(mysql_config)
        self._tables = tuple(tables)

    @property
    def assignments_table(self) -> str:
        """Return the table name that stores tag assignments."""

        return self._tables[0]

    def read_assignments(self, limit: Optional[int] = None) -> List[TagAssignment]:
        """Load tag assignments from MySQL and convert them into domain objects."""

        rows = self._importer.fetch_table(self.assignments_table, limit=limit)
        table_data = {self.assignments_table: list(rows)}
        assignments, _ = self._build_assignments(table_data[self.assignments_table], table_data)
        return assignments

    def read_domain_objects(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Load assignments and derive the primary domain objects from them."""

        table_data = self._importer.import_tables(self._tables, limit=limit)
        assignment_rows = table_data.get(self.assignments_table, [])
        assignments, metadata = self._build_assignments(assignment_rows, table_data)

        comments = self._build_comments(metadata, assignments)
        taggers = self._build_taggers(metadata, assignments)
        characteristics = self._build_characteristics(metadata)
        answers = self._build_answers(metadata)

        return {
            "assignments": assignments,
            "comments": comments,
            "taggers": taggers,
            "characteristics": characteristics,
            "answers": answers,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_assignments(
        self,
        rows: Iterable[Mapping[str, Any]],
        table_data: Optional[Mapping[str, Sequence[Mapping[str, Any]]]] = None,
    ) -> tuple[List[TagAssignment], Dict[str, Any]]:
        """Convert raw MySQL rows into TagAssignment objects.

        The method also collects metadata about comments, taggers and
        characteristics which is later used to build the other domain objects.
        """

        answers_lookup: Dict[str, Mapping[str, Any]] = {}
        if table_data:
            answers_rows = table_data.get("answers") or []
            for answer in answers_rows:
                answer_id = self._extract_optional(
                    answer,
                    ["id", "answer_id", "answerId"],
                )
                if answer_id in (None, ""):
                    continue
                answers_lookup[str(answer_id)] = answer

        assignments: List[TagAssignment] = []
        assignments_by_comment: DefaultDict[str, List[TagAssignment]] = defaultdict(list)
        assignments_by_tagger: DefaultDict[str, List[TagAssignment]] = defaultdict(list)
        comment_meta: Dict[str, Dict[str, Any]] = {}
        characteristic_meta: Dict[str, Dict[str, Any]] = {}
        tagger_meta: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            if row is None:
                raise ValueError("Invalid assignment row: None")

            try:
                assignment = self._row_to_assignment(row)
            except (KeyError, ValueError, TypeError) as exc:  # pragma: no cover - defensive
                raise ValueError(f"Invalid assignment row: {row!r}") from exc

            if not isinstance(assignment, TagAssignment):  # pragma: no cover - defensive
                raise ValueError(f"Invalid assignment row: {row!r}")

            assignments.append(assignment)
            assignments_by_comment[assignment.comment_id].append(assignment)
            assignments_by_tagger[assignment.tagger_id].append(assignment)

            comment_id = assignment.comment_id
            answer_row = answers_lookup.get(comment_id)
            comment_text = None
            prompt_id: Optional[Any] = None
            question_id: Optional[Any] = None
            response_id: Optional[Any] = None
            answer_value: Optional[Any] = None

            if answer_row:
                comment_text = self._extract_optional(
                    answer_row,
                    ["comments", "comment", "answer", "text", "body"],
                )
                prompt_id = self._extract_optional(
                    answer_row,
                    ["question_id", "prompt_id", "response_id"],
                )
                question_id = self._extract_optional(answer_row, ["question_id"])
                response_id = self._extract_optional(answer_row, ["response_id"])
                answer_value = self._extract_optional(answer_row, ["answer", "value"])

            if not comment_text:
                comment_text = self._extract_optional(row, ["comment_text", "text", "body"])
            if not comment_text:
                comment_text = comment_id

            if prompt_id in (None, ""):
                prompt_id = self._extract_optional(row, ["prompt_id", "promptId"])
            if prompt_id in (None, ""):
                prompt_id = question_id or response_id or "unknown_prompt"

            comment_meta[comment_id] = {
                "text": str(comment_text),
                "prompt_id": str(prompt_id) if prompt_id not in (None, "") else "unknown_prompt",
                "question_id": str(question_id) if question_id not in (None, "") else None,
                "response_id": str(response_id) if response_id not in (None, "") else None,
                "answer_value": answer_value,
            }

            characteristic_id = assignment.characteristic_id
            characteristic_name = self._extract_optional(
                row,
                [
                    "characteristic_name",
                    "characteristic",
                    "tag_prompt_characteristic",
                ],
            )
            if not characteristic_name:
                characteristic_name = characteristic_id
            characteristic_description = self._extract_optional(
                row,
                ["characteristic_description", "description"],
            )
            characteristic_meta[characteristic_id] = {
                "name": characteristic_name,
                "description": characteristic_description,
            }

            tagger_id = assignment.tagger_id
            tagger_entry = tagger_meta.setdefault(tagger_id, {})
            for key in ("team_id", "tagger_team", "tagger_meta"):
                if key in row and row[key] is not None:
                    tagger_entry.setdefault(key, row[key])

        metadata = {
            "assignments_by_comment": assignments_by_comment,
            "assignments_by_tagger": assignments_by_tagger,
            "comment_meta": comment_meta,
            "characteristic_meta": characteristic_meta,
            "tagger_meta": tagger_meta,
            "answers_by_id": answers_lookup,
        }
        return assignments, metadata

    def _build_comments(
        self,
        metadata: Mapping[str, Any],
        assignments: Iterable[TagAssignment],
    ) -> List[Comment]:
        assignments_by_comment: Mapping[str, List[TagAssignment]] = metadata["assignments_by_comment"]
        comment_meta: Mapping[str, Mapping[str, Any]] = metadata["comment_meta"]

        comments: List[Comment] = []
        for comment_id, info in comment_meta.items():
            comment_assignments = assignments_by_comment.get(comment_id, [])
            comments.append(
                Comment(
                    id=comment_id,
                    text=str(info.get("text", comment_id)),
                    prompt_id=str(info.get("prompt_id", "unknown_prompt")),
                    tagassignments=list(comment_assignments),
                )
            )
        return comments

    def _build_taggers(
        self,
        metadata: Mapping[str, Any],
        assignments: Iterable[TagAssignment],
    ) -> List[Tagger]:
        assignments_by_tagger: Mapping[str, List[TagAssignment]] = metadata["assignments_by_tagger"]
        tagger_meta: Mapping[str, Mapping[str, Any]] = metadata["tagger_meta"]

        taggers: List[Tagger] = []
        for tagger_id, info in tagger_meta.items():
            meta = dict(info)
            if not meta:
                meta = None
            taggers.append(
                Tagger(
                    id=tagger_id,
                    meta=meta,
                    tagassignments=list(assignments_by_tagger.get(tagger_id, [])),
                )
            )
        return taggers

    def _build_characteristics(self, metadata: Mapping[str, Any]) -> List[Characteristic]:
        characteristic_meta: Mapping[str, Mapping[str, Any]] = metadata["characteristic_meta"]

        characteristics: List[Characteristic] = []
        for char_id, info in characteristic_meta.items():
            characteristics.append(
                Characteristic(
                    id=char_id,
                    name=str(info.get("name", char_id)),
                    description=info.get("description"),
                )
            )
        return characteristics

    def _build_answers(self, metadata: Mapping[str, Any]) -> List[Dict[str, Any]]:
        answers_by_id: Mapping[str, Mapping[str, Any]] = metadata.get("answers_by_id", {})
        comment_meta: Mapping[str, Mapping[str, Any]] = metadata.get("comment_meta", {})

        answers: List[Dict[str, Any]] = []
        for answer_id in answers_by_id:
            info = comment_meta.get(answer_id, {})
            question_id = info.get("question_id")
            response_id = info.get("response_id")
            answer_value = info.get("answer_value")
            text = info.get("text", answer_id)
            answers.append(
                {
                    "id": answer_id,
                    "question_id": str(question_id) if question_id not in (None, "") else None,
                    "response_id": str(response_id) if response_id not in (None, "") else None,
                    "text": str(text),
                    "answer_value": answer_value,
                }
            )

        if not answers and comment_meta:
            for answer_id, info in comment_meta.items():
                answers.append(
                    {
                        "id": answer_id,
                        "question_id": info.get("question_id"),
                        "response_id": info.get("response_id"),
                        "text": str(info.get("text", answer_id)),
                        "answer_value": info.get("answer_value"),
                    }
                )

        return answers

    def _row_to_assignment(self, row: Mapping[str, Any]) -> TagAssignment:
        tagger_id = str(
            self._extract_required(row, ["tagger_id", "worker_id", "user_id"])
        )
        comment_id = str(
            self._extract_required(
                row,
                [
                    "comment_id",
                    "commentId",
                    "item_id",
                    "answer_id",
                    "answerId",
                ],
            )
        )
        characteristic_id = str(
            self._extract_required(
                row,
                [
                    "characteristic_id",
                    "characteristicId",
                    "tag_prompt_deployment_characteristic_id",
                    "tag_prompt_deployment_id",
                ],
            )
        )
        value_raw = self._extract_required(row, ["value", "answer", "tag_value"])
        tag_value = self._parse_tag_value(value_raw)
        timestamp_raw = self._extract_optional(
            row,
            ["tagged_at", "created_at", "updated_at", "timestamp"],
        )
        timestamp = self._parse_timestamp(timestamp_raw)

        return TagAssignment(
            tagger_id=tagger_id,
            comment_id=comment_id,
            characteristic_id=characteristic_id,
            value=tag_value,
            timestamp=timestamp,
        )

    _NUMERIC_TAG_VALUE_MAP = {
        0: TagValue.NO,
        1: TagValue.YES,
        2: TagValue.NA,
        3: TagValue.UNCERTAIN,
        4: TagValue.SKIP,
    }

    _NEGATIVE_TAG_VALUE_MAP = {
        -1: TagValue.NA,
    }

    _TEXT_TAG_VALUE_MAP = {
        "TRUE": TagValue.YES,
        "FALSE": TagValue.NO,
        "T": TagValue.YES,
        "F": TagValue.NO,
        "Y": TagValue.YES,
        "N": TagValue.NO,
    }

    def _parse_tag_value(self, value: Any) -> TagValue:
        if isinstance(value, TagValue):
            return value
        if value is None:
            raise ValueError("Tag value cannot be None")

        if isinstance(value, (bytes, bytearray, memoryview)):
            text = bytes(value).decode("utf-8", errors="ignore").strip()
        else:
            text = str(value).strip()

        if not text:
            raise ValueError("Tag value cannot be empty")

        normalized = text.upper()

        if normalized in self._TEXT_TAG_VALUE_MAP:
            return self._TEXT_TAG_VALUE_MAP[normalized]

        numeric_value: Optional[int]
        try:
            numeric_value = int(float(normalized))
        except ValueError:
            numeric_value = None

        if numeric_value is not None:
            if numeric_value in self._NUMERIC_TAG_VALUE_MAP:
                return self._NUMERIC_TAG_VALUE_MAP[numeric_value]
            if numeric_value in self._NEGATIVE_TAG_VALUE_MAP:
                return self._NEGATIVE_TAG_VALUE_MAP[numeric_value]

        try:
            return TagValue(normalized)
        except ValueError as exc:  # pragma: no cover - defensive
            raise ValueError(f"Unsupported tag value: {value!r}") from exc

    def _parse_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text)
            except ValueError:
                pass
        raise ValueError(f"Cannot parse timestamp from value: {value!r}")

    def _extract_required(self, row: Mapping[str, Any], keys: Sequence[str]) -> Any:
        value = self._extract_optional(row, keys)
        if value in (None, ""):
            raise KeyError(f"Missing required columns {keys!r} in row {row!r}")
        return value

    def _extract_optional(self, row: Mapping[str, Any], keys: Sequence[str]) -> Optional[Any]:
        for key in keys:
            value = self._get_column_value(row, key)
            if value not in (None, ""):
                return value
        return None

    def _get_column_value(self, row: Mapping[str, Any], key: str) -> Optional[Any]:
        if key in row:
            return row[key]
        if not isinstance(key, str):
            return None
        key_normalized = self._normalize_column_name(key)
        for column, value in row.items():
            if isinstance(column, str) and self._normalize_column_name(column) == key_normalized:
                return value
        return None

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        return "".join(ch for ch in name.lower() if ch.isalnum())

