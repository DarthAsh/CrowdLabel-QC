"""CSV adapter for reading and writing crowd labeling data."""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from qcc.domain.characteristic import Characteristic
from qcc.domain.comment import Comment
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger


class CSVAdapter:
    """Adapter for reading and writing crowd labeling data in CSV format.
    
    Handles the canonical CSV format with columns:
    assignment_id, team_id, tagger_id, comment_id, prompt_id, characteristic,
    value, tagged_at, comment_text, prompt_text
    """
    
    CANONICAL_COLUMNS = [
        "assignment_id",
        "team_id", 
        "tagger_id",
        "comment_id",
        "prompt_id",
        "characteristic",
        "value",
        "tagged_at",
        "comment_text",
        "prompt_text"
    ]
    
    def read_assignments(self, file_path: Path) -> List[TagAssignment]:
        """Read tag assignments from a CSV file.
        
        Args:
            file_path: Path to the CSV file to read
            
        Returns:
            List of TagAssignment objects
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the CSV format is invalid
        """
        rows = self._read_rows(file_path)
        return [self._row_to_assignment(row) for row in rows]
    
    def write_assignments(self, assignments: List[TagAssignment], file_path: Path) -> None:
        """Write tag assignments to a CSV file.
        
        Args:
            assignments: List of TagAssignment objects to write
            file_path: Path where to write the CSV file
        """
        # TODO: Implement CSV writing logic
        raise NotImplementedError("write_assignments not yet implemented")
    
    def read_domain_objects(self, file_path: Path) -> Dict[str, any]:
        """Read all domain objects from a CSV file.
        
        Args:
            file_path: Path to the CSV file to read
            
        Returns:
            Dictionary containing:
            - 'assignments': List of TagAssignment objects
            - 'comments': List of Comment objects  
            - 'taggers': List of Tagger objects
            - 'characteristics': List of Characteristic objects
        """
        rows = self._read_rows(file_path)
        assignments = [self._row_to_assignment(row) for row in rows]

        assignments_by_comment: Dict[str, List[TagAssignment]] = defaultdict(list)
        assignments_by_tagger: Dict[str, List[TagAssignment]] = defaultdict(list)
        comment_meta: Dict[str, Dict[str, Optional[str]]] = {}
        characteristic_meta: Dict[str, Dict[str, Optional[str]]] = {}
        tagger_meta: Dict[str, Dict[str, Optional[str]]] = {}

        for assignment, row in zip(assignments, rows):
            assignments_by_comment[assignment.comment_id].append(assignment)
            assignments_by_tagger[assignment.tagger_id].append(assignment)

            comment_id = assignment.comment_id
            comment_meta.setdefault(
                comment_id,
                {
                    "text": row.get("comment_text") or row.get("comment") or comment_id,
                    "prompt_id": row.get("prompt_id") or "",  # placeholder, filled below
                    "prompt_text": row.get("prompt_text") or None,
                },
            )
            if not comment_meta[comment_id]["prompt_id"]:
                comment_meta[comment_id]["prompt_id"] = row.get("prompt") or "unknown_prompt"

            characteristic_id = assignment.characteristic_id
            characteristic_meta.setdefault(
                characteristic_id,
                {
                    "name": row.get("characteristic_name")
                    or row.get("characteristic")
                    or characteristic_id,
                    "description": row.get("characteristic_description") or None,
                },
            )

            tagger_id = assignment.tagger_id
            tagger_info = tagger_meta.setdefault(tagger_id, {})
            if row.get("team_id"):
                tagger_info.setdefault("team_id", row.get("team_id"))
            if row.get("tagger_meta"):
                tagger_info.setdefault("tagger_meta", row.get("tagger_meta"))

        comments = [
            Comment(
                id=comment_id,
                text=str(meta.get("text") or comment_id),
                prompt_id=str(meta.get("prompt_id") or "unknown_prompt"),
                tagassignments=list(assignments_by_comment.get(comment_id, [])),
            )
            for comment_id, meta in comment_meta.items()
        ]

        taggers = [
            Tagger(
                id=tagger_id,
                meta=info or None,
                tagassignments=list(assignments_by_tagger.get(tagger_id, [])),
            )
            for tagger_id, info in tagger_meta.items()
        ]

        characteristics = [
            Characteristic(
                id=char_id,
                name=str(meta.get("name") or char_id),
                description=meta.get("description"),
            )
            for char_id, meta in characteristic_meta.items()
        ]

        return {
            "assignments": assignments,
            "comments": comments,
            "taggers": taggers,
            "characteristics": characteristics,
            "prompts": [],
        }
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object.
        
        Args:
            timestamp_str: String representation of timestamp
            
        Returns:
            Parsed datetime object
        """
        if not timestamp_str:
            raise ValueError("timestamp cannot be empty")

        text = timestamp_str.strip()
        if not text:
            raise ValueError("timestamp cannot be empty")

        if text.endswith("Z"):
            try:
                return datetime.fromisoformat(text[:-1] + "+00:00")
            except ValueError:
                pass

        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        raise ValueError(f"Unsupported timestamp format: {timestamp_str!r}")
    
    def _parse_tag_value(self, value_str: str) -> TagValue:
        """Parse tag value string to TagValue enum.
        
        Args:
            value_str: String representation of tag value
            
        Returns:
            Corresponding TagValue enum value
            
        Raises:
            ValueError: If the value string is not valid
        """
        if value_str is None:
            raise ValueError("tag value cannot be empty")

        text = str(value_str).strip().upper()
        if not text:
            raise ValueError("tag value cannot be empty")

        try:
            return TagValue(text)
        except ValueError as exc:
            raise ValueError(f"Unsupported tag value: {value_str!r}") from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _read_rows(self, file_path: Path) -> List[Dict[str, str]]:
        """Read all rows from the CSV file and return them as dictionaries."""

        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError("CSV file is missing a header row")
            missing_columns = [
                column
                for column in ("tagger_id", "comment_id", "characteristic", "value", "tagged_at")
                if column not in reader.fieldnames
            ]
            if missing_columns:
                raise ValueError(
                    "CSV file is missing required columns: " + ", ".join(missing_columns)
                )

            return [dict(row) for row in reader]

    def _row_to_assignment(self, row: Dict[str, str]) -> TagAssignment:
        """Convert a CSV row into a ``TagAssignment`` instance."""

        tagger_id = str(row.get("tagger_id", "")).strip()
        comment_id = str(row.get("comment_id", "")).strip()
        characteristic_id = str(row.get("characteristic", "")).strip()
        value_raw = row.get("value")
        timestamp_raw = row.get("tagged_at")

        assignment_id = row.get("assignment_id")
        if assignment_id is not None:
            assignment_id = str(assignment_id).strip() or None

        prompt_id = row.get("prompt_id") or row.get("prompt")
        if prompt_id is not None:
            prompt_id = str(prompt_id).strip() or None

        team_id = row.get("team_id")
        if team_id is not None:
            team_id = str(team_id).strip() or None

        if not tagger_id or not comment_id or not characteristic_id:
            raise ValueError(f"Missing required identifiers in row: {row!r}")

        if timestamp_raw is None:
            raise ValueError(f"Missing tagged_at timestamp in row: {row!r}")

        tag_value = self._parse_tag_value(value_raw)
        timestamp = self._parse_timestamp(timestamp_raw)

        return TagAssignment(
            tagger_id=tagger_id,
            comment_id=comment_id,
            characteristic_id=characteristic_id,
            value=tag_value,
            timestamp=timestamp,
            assignment_id=assignment_id,
            prompt_id=prompt_id,
            team_id=team_id,
        )
