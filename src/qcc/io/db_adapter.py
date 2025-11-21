"""Database adapter for reading crowd labeling data from MySQL."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import replace
from datetime import datetime
from typing import Any, DefaultDict, Dict, Iterable, List, Mapping, Optional, Sequence
from typing import NamedTuple

from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.data_ingestion.mysql_importer import DEFAULT_TAG_PROMPT_TABLES, TableImporter
from qcc.domain.characteristic import Characteristic
from qcc.domain.comment import Comment
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.domain.enums import TagValue


logger = logging.getLogger(__name__)


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

    def read_assignments_from_questionnaires(
        self, limit: Optional[int] = None
    ) -> List[TagAssignment]:
        """Load assignments by walking the questionnaire → question → answer chain."""

        table_data = self._import_questionnaire_root_tables(limit=limit)
        assignment_rows = table_data.get(self.assignments_table, [])
        assignments, _ = self._build_assignments(assignment_rows, table_data)
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
        prompt_deployments = self._build_prompt_deployments(metadata)
        prompts = self._build_prompts(metadata)
        questions = self._build_questions(metadata)

        return {
            "assignments": assignments,
            "comments": comments,
            "taggers": taggers,
            "characteristics": characteristics,
            "answers": answers,
            "prompt_deployments": prompt_deployments,
            "prompts": prompts,
            "questions": questions,
        }

    def read_domain_objects_from_questionnaires(
        self, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Load assignments anchored on questionnaires and derive domain objects."""

        table_data = self._import_questionnaire_root_tables(limit=limit)
        assignment_rows = table_data.get(self.assignments_table, [])
        assignments, metadata = self._build_assignments(assignment_rows, table_data)

        comments = self._build_comments(metadata, assignments)
        taggers = self._build_taggers(metadata, assignments)
        characteristics = self._build_characteristics(metadata)
        answers = self._build_answers(metadata)
        prompt_deployments = self._build_prompt_deployments(metadata)
        prompts = self._build_prompts(metadata)
        questions = self._build_questions(metadata)

        return {
            "assignments": assignments,
            "comments": comments,
            "taggers": taggers,
            "characteristics": characteristics,
            "answers": answers,
            "prompt_deployments": prompt_deployments,
            "prompts": prompts,
            "questions": questions,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _import_questionnaire_root_tables(
        self, limit: Optional[int] = None
    ) -> Mapping[str, List[Mapping[str, Any]]]:
        """Import tables by iterating questionnaires first and filtering dependents."""

        logger.info("Importing assignment_questionnaires with limit=%s", limit)
        questionnaire_rows = self._importer.fetch_table(
            "assignment_questionnaires", limit=limit
        )
        questionnaire_rows = [
            row
            for row in questionnaire_rows
            if str(
                self._extract_optional(row, ["assignment_id", "assignmentId"])
            )
            == "1205"
        ]
        logger.info(
            "Fetched %d assignment_questionnaire rows scoped to assignment_id=1205",
            len(questionnaire_rows),
        )
        questionnaire_ids = {
            str(qid)
            for row in questionnaire_rows
            if (qid := self._extract_optional(row, ["questionnaire_id", "questionnaireId"]))
            not in (None, "")
        }

        logger.info("Importing questions for %d questionnaires", len(questionnaire_ids))
        questions_rows = self._importer.fetch_table("questions")
        questions_filtered = [
            row
            for row in questions_rows
            if str(
                self._extract_optional(row, ["questionnaire_id", "questionnaireId"])
            )
            in questionnaire_ids
        ]
        question_ids = {
            str(qid)
            for row in questions_filtered
            if (qid := self._extract_optional(row, ["id", "question_id", "questionId"]))
            not in (None, "")
        }

        logger.info("Importing answers for %d questions", len(question_ids))
        answers_rows = self._importer.fetch_table("answers")
        answers_filtered = [
            row
            for row in answers_rows
            if str(self._extract_optional(row, ["question_id", "questionId"]))
            in question_ids
        ]
        answer_ids = {
            str(aid)
            for row in answers_filtered
            if (aid := self._extract_optional(row, ["id", "answer_id", "answerId"]))
            not in (None, "")
        }

        logger.info("Importing assignments for %d answers", len(answer_ids))
        assignment_rows = self._importer.fetch_table(self.assignments_table)
        assignments_filtered = []
        for row in assignment_rows:
            comment_id = self._extract_optional(
                row, ["comment_id", "item_id", "answer_id", "answerId"]
            )
            if str(comment_id) in answer_ids:
                assignments_filtered.append(row)

        logger.info(
            "Filtered %d/%d assignments linked to questionnaire answers",
            len(assignments_filtered),
            len(assignment_rows),
        )

        logger.info("Importing tag_prompt_deployments for %d questions", len(question_ids))
        deployments_rows = self._importer.fetch_table("tag_prompt_deployments")
        deployments_filtered = [
            row
            for row in deployments_rows
            if str(self._extract_optional(row, ["assignment_id", "question_id", "questionId"]))
            in question_ids
            or str(self._extract_optional(row, ["questionnaire_id", "questionnaireId"]))
            in questionnaire_ids
        ]

        prompt_ids = {
            str(pid)
            for row in deployments_filtered
            if (pid := self._extract_optional(row, ["tag_prompt_id", "tagPromptId"]))
            not in (None, "")
        }
        logger.info(
            "Importing tag_prompts for %d deployments", len(deployments_filtered)
        )
        prompts_rows = self._importer.fetch_table("tag_prompts")
        prompts_filtered = [
            row
            for row in prompts_rows
            if str(self._extract_optional(row, ["id", "tag_prompt_id", "tagPromptId"]))
            in prompt_ids
        ]

        logger.info(
            "Questionnaire-root import complete: %d questionnaires, %d questions, %d answers, %d assignments, %d deployments, %d prompts",
            len(questionnaire_rows),
            len(questions_filtered),
            len(answers_filtered),
            len(assignments_filtered),
            len(deployments_filtered),
            len(prompts_filtered),
        )

        return {
            "assignment_questionnaires": questionnaire_rows,
            "questions": questions_filtered,
            "answers": answers_filtered,
            "tag_prompt_deployments": deployments_filtered,
            "tag_prompts": prompts_filtered,
            self.assignments_table: assignments_filtered,
        }

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
        deployments_lookup: Dict[str, Mapping[str, Any]] = {}
        deployments_by_question_id: Dict[str, str] = {}
        prompts_lookup: Dict[str, Mapping[str, Any]] = {}
        questions_lookup: Dict[str, Mapping[str, Any]] = {}
        assignment_questionnaires_by_questionnaire: Dict[str, Dict[str, Optional[str]]] = {}

        total_assignments: Optional[int] = None
        try:
            total_assignments = len(rows)  # type: ignore[arg-type]
        except TypeError:
            pass
        if total_assignments:
            logger.info("Starting assignment ingestion for %d rows", total_assignments)
        else:
            logger.info("Starting assignment ingestion")
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

            deployments_rows = table_data.get("tag_prompt_deployments") or []
            for deployment in deployments_rows:
                deployment_id = self._extract_optional(
                    deployment,
                    ["id", "tag_prompt_deployment_id", "tagPromptDeploymentId"],
                )
                if deployment_id in (None, ""):
                    continue
                deployments_lookup[str(deployment_id)] = deployment

                question_id = self._extract_optional(
                    deployment, ["assignment_id", "question_id", "questionId"]
                )
                if question_id not in (None, "") and str(question_id) not in deployments_by_question_id:
                    deployments_by_question_id[str(question_id)] = str(deployment_id)

            prompts_rows = table_data.get("tag_prompts") or []
            for prompt in prompts_rows:
                prompt_id = self._extract_optional(
                    prompt,
                    ["id", "tag_prompt_id", "tagPromptId"],
                )
                if prompt_id in (None, ""):
                    continue
                prompts_lookup[str(prompt_id)] = prompt

            questions_rows = table_data.get("questions") or []
            for question in questions_rows:
                question_id = self._extract_optional(
                    question,
                    ["id", "question_id", "questionId"],
                )
                if question_id in (None, ""):
                    continue
                questions_lookup[str(question_id)] = question

            questionnaire_rows = table_data.get("assignment_questionnaires") or []
            for questionnaire in questionnaire_rows:
                assignment_id = self._extract_optional(
                    questionnaire,
                    ["assignment_id", "assignmentId"],
                )
                questionnaire_id = self._extract_optional(
                    questionnaire,
                    ["questionnaire_id", "questionnaireId"],
                )
                user_id = self._extract_optional(questionnaire, ["user_id", "userId"])
                if assignment_id in (None, "") or questionnaire_id in (None, ""):
                    continue
                assignment_questionnaires_by_questionnaire[str(questionnaire_id)] = {
                    "assignment_id": str(assignment_id),
                    "user_id": str(user_id) if user_id not in (None, "") else None,
                }

        assignments: List[TagAssignment] = []
        assignments_by_comment: DefaultDict[str, List[TagAssignment]] = defaultdict(list)
        assignments_by_tagger: DefaultDict[str, List[TagAssignment]] = defaultdict(list)
        comment_meta: Dict[str, Dict[str, Any]] = {}
        characteristic_meta: Dict[str, Dict[str, Any]] = {}
        tagger_meta: Dict[str, Dict[str, Any]] = {}

        assignment_id_sources = {
            "questionnaire": 0,
            "deployment": 0,
            "row": 0,
            "missing": 0,
        }

        skipped_missing_tagger = 0

        for idx, row in enumerate(rows, 1):
            if row is None:
                raise ValueError("Invalid assignment row: None")

            try:
                parsed = self._parse_assignment_fields(row)

                deployment_row = deployments_lookup.get(parsed.characteristic_id)
                deployment_assignment_id: Optional[Any] = None
                if deployment_row:
                    deployment_assignment_id = self._extract_optional(
                        deployment_row, ["assignment_id", "question_id", "questionId"]
                    )

                answer_row = answers_lookup.get(parsed.comment_id)
                answer_question_id = None
                if answer_row:
                    answer_question_id = self._extract_optional(
                        answer_row, ["question_id", "questionId"]
                    )
                questionnaire_id = None
                if answer_question_id not in (None, ""):
                    question_row = questions_lookup.get(str(answer_question_id))
                    if question_row:
                        questionnaire_id = self._extract_optional(
                            question_row, ["questionnaire_id", "questionnaireId"]
                        )
                questionnaire_assignment_id = None
                questionnaire_user_id: Optional[Any] = None
                if questionnaire_id not in (None, ""):
                    questionnaire_assignment_entry = assignment_questionnaires_by_questionnaire.get(
                        str(questionnaire_id)
                    )
                    if questionnaire_assignment_entry:
                        questionnaire_assignment_id = questionnaire_assignment_entry.get(
                            "assignment_id"
                        )
                        questionnaire_user_id = questionnaire_assignment_entry.get(
                            "user_id"
                        )

                assignment_id_override = questionnaire_assignment_id
                assignment_id_source = "questionnaire"
                if assignment_id_override in (None, ""):
                    assignment_id_override = deployment_assignment_id
                    assignment_id_source = "deployment"
                if assignment_id_override in (None, ""):
                    assignment_id_override = parsed.assignment_id
                    assignment_id_source = "row"
                if assignment_id_override in (None, ""):
                    assignment_id_source = "missing"

                assignment_id_sources[assignment_id_source] += 1

                tagger_id_override = parsed.tagger_id
                if tagger_id_override in (None, ""):
                    tagger_id_override = questionnaire_user_id
                if tagger_id_override in (None, ""):
                    skipped_missing_tagger += 1
                    logger.warning(
                        "Skipping assignment row with no user_id/tagger after questionnaire backfill: %r",
                        row,
                    )
                    continue

                assignment = self._row_to_assignment(
                    row,
                    tagger_id_override=str(tagger_id_override),
                    assignment_id_override=assignment_id_override,
                )

            except (KeyError, ValueError, TypeError) as exc:  # pragma: no cover - defensive
                user_identifier: Optional[Any]
                try:
                    user_identifier = self._extract_optional(
                        row, ["tagger_id", "worker_id", "user_id"]
                    )
                except Exception:  # pragma: no cover - defensive fallback
                    user_identifier = None
                logger.error(
                    "Unable to parse MySQL assignment row for user_id=%s (%s: %s)",
                    user_identifier if user_identifier not in (None, "") else "<unknown>",
                    type(exc).__name__,
                    exc,
                    exc_info=exc,
                )
                raise ValueError(f"Invalid assignment row: {row!r}") from exc

            if not isinstance(assignment, TagAssignment):  # pragma: no cover - defensive
                raise ValueError(f"Invalid assignment row: {row!r}")

            self._record_assignment(
                assignment=assignment,
                row=row,
                answers_lookup=answers_lookup,
                deployments_lookup=deployments_lookup,
                prompts_lookup=prompts_lookup,
                questions_lookup=questions_lookup,
                comment_meta=comment_meta,
                characteristic_meta=characteristic_meta,
                tagger_meta=tagger_meta,
                assignments=assignments,
                assignments_by_comment=assignments_by_comment,
                assignments_by_tagger=assignments_by_tagger,
            )

            if idx % 1000 == 0:
                if total_assignments:
                    logger.info(
                        "Processed %d/%d assignment rows", idx, total_assignments
                    )
                else:
                    logger.info("Processed %d assignment rows", idx)

        missing_answer_ids = set(answers_lookup.keys()) - set(assignments_by_comment.keys())
        if missing_answer_ids:
            logger.info(
                "Adding default SKIP tags for %d answers without assignments",
                len(missing_answer_ids),
            )
        for answer_id in missing_answer_ids:
            answer_row = answers_lookup[answer_id]
            answer_question_id = self._extract_optional(
                answer_row, ["question_id", "questionId"]
            )
            questionnaire_id = None
            if answer_question_id not in (None, ""):
                question_row = questions_lookup.get(str(answer_question_id))
                if question_row:
                    questionnaire_id = self._extract_optional(
                        question_row, ["questionnaire_id", "questionnaireId"]
                    )

            questionnaire_entry = None
            if questionnaire_id not in (None, ""):
                questionnaire_entry = assignment_questionnaires_by_questionnaire.get(
                    str(questionnaire_id)
                )

            tagger_id_override = None
            assignment_id_override = None
            if questionnaire_entry:
                tagger_id_override = questionnaire_entry.get("user_id")
                assignment_id_override = questionnaire_entry.get("assignment_id")
                assignment_id_sources["questionnaire"] += 1
            else:
                assignment_id_sources["missing"] += 1

            if tagger_id_override in (None, ""):
                skipped_missing_tagger += 1
                logger.warning(
                    "Skipping default SKIP assignment for answer %s due to missing user_id",
                    answer_id,
                )
                continue

            characteristic_id = None
            if answer_question_id not in (None, ""):
                characteristic_id = deployments_by_question_id.get(str(answer_question_id))
            if characteristic_id in (None, "") and deployments_lookup:
                characteristic_id = next(iter(deployments_lookup.keys()))
            if characteristic_id in (None, ""):
                logger.warning(
                    "Skipping default SKIP assignment for answer %s due to missing characteristic mapping",
                    answer_id,
                )
                continue

            deployment_row = deployments_lookup.get(characteristic_id)
            synthetic_timestamp = self._extract_optional(
                answer_row,
                ["created_at", "createdAt", "updated_at", "updatedAt", "timestamp"],
            )
            if synthetic_timestamp in (None, ""):
                synthetic_timestamp = datetime.utcnow()

            synthetic_row = {
                "comment_id": answer_id,
                "characteristic_id": characteristic_id,
                "value": 0,
                "tagged_at": synthetic_timestamp,
                "tagger_id": tagger_id_override,
                "assignment_id": assignment_id_override,
                "prompt_id": self._extract_optional(
                    deployment_row or {},
                    ["tag_prompt_id", "tagPromptId", "prompt_id", "promptId"],
                )
                or answer_question_id,
            }

            try:
                synthetic_assignment = self._row_to_assignment(
                    synthetic_row,
                    tagger_id_override=str(tagger_id_override),
                    assignment_id_override=assignment_id_override,
                )
            except (KeyError, ValueError, TypeError) as exc:  # pragma: no cover - defensive
                logger.error(
                    "Unable to build synthetic SKIP assignment for answer %s (%s: %s)",
                    answer_id,
                    type(exc).__name__,
                    exc,
                    exc_info=exc,
                )
                continue

            self._record_assignment(
                assignment=synthetic_assignment,
                row=synthetic_row,
                answers_lookup=answers_lookup,
                deployments_lookup=deployments_lookup,
                prompts_lookup=prompts_lookup,
                questions_lookup=questions_lookup,
                comment_meta=comment_meta,
                characteristic_meta=characteristic_meta,
                tagger_meta=tagger_meta,
                assignments=assignments,
                assignments_by_comment=assignments_by_comment,
                assignments_by_tagger=assignments_by_tagger,
            )

        logger.info(
            "Finished assignment ingestion: built %d assignments across %d taggers",
            len(assignments),
            len(assignments_by_tagger),
        )
        logger.info(
            "Assignment ID resolution sources — questionnaires: %d, deployments: %d, rows: %d, missing: %d",
            assignment_id_sources["questionnaire"],
            assignment_id_sources["deployment"],
            assignment_id_sources["row"],
            assignment_id_sources["missing"],
        )

        if skipped_missing_tagger:
            logger.info(
                "Skipped %d assignment rows that did not include a user_id",
                skipped_missing_tagger,
            )

        if assignments:
            sample_assignment = assignments[0]
            comment_id = sample_assignment.comment_id
            comment_entry = comment_meta.get(comment_id, {})

            answer_row = answers_lookup.get(comment_id)
            question_id: Optional[str] = None
            questionnaire_id: Optional[str] = None
            answer_payload = None
            if answer_row:
                question_id = self._extract_optional(answer_row, ["question_id", "questionId"])
                questionnaire_id = self._extract_optional(
                    answer_row, ["questionnaire_id", "questionnaireId"]
                )
                answer_payload = {
                    "id": comment_id,
                    "question_id": question_id,
                    "questionnaire_id": questionnaire_id,
                    "answer": self._extract_optional(
                        answer_row, ["comments", "comment", "answer", "text", "body"]
                    ),
                    "value": self._extract_optional(answer_row, ["answer", "value"]),
                }

            if not question_id:
                question_id = comment_entry.get("question_id")
            if question_id and not questionnaire_id:
                question_row = questions_lookup.get(str(question_id))
                if question_row:
                    questionnaire_id = self._extract_optional(
                        question_row, ["questionnaire_id", "questionnaireId"]
                    )
            question_payload = None
            if question_id:
                question_row = questions_lookup.get(str(question_id))
                if question_row:
                    question_payload = {
                        "id": str(question_id),
                        "text": self._extract_optional(
                            question_row, ["txt", "text", "question", "prompt"]
                        ),
                        "questionnaire_id": questionnaire_id,
                        "type": question_row.get("type"),
                    }

            def _assignment_payload(assignment: TagAssignment) -> Dict[str, Any]:
                return {
                    "assignment_id": assignment.assignment_id,
                    "comment_id": assignment.comment_id,
                    "characteristic_id": assignment.characteristic_id,
                    "tagger_id": assignment.tagger_id,
                    "value": assignment.value.name if hasattr(assignment.value, "name") else assignment.value,
                    "timestamp": assignment.timestamp,
                }

            logger.info(
                "Sample assignment and related data: assignment=%s; answer=%s; question=%s; answer_tags=%s",
                _assignment_payload(sample_assignment),
                answer_payload,
                question_payload,
                [_assignment_payload(a) for a in assignments_by_comment.get(comment_id, [])],
            )

        metadata = {
            "assignments_by_comment": assignments_by_comment,
            "assignments_by_tagger": assignments_by_tagger,
            "comment_meta": comment_meta,
            "characteristic_meta": characteristic_meta,
            "tagger_meta": tagger_meta,
            "answers_by_id": answers_lookup,
            "deployments_by_id": deployments_lookup,
            "prompts_by_id": prompts_lookup,
            "questions_by_id": questions_lookup,
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
                    "questionnaire_id": info.get("questionnaire_id"),
                    "question_type": info.get("question_type"),
                    "question_text": info.get("question_text"),
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
                        "questionnaire_id": info.get("questionnaire_id"),
                        "question_type": info.get("question_type"),
                        "question_text": info.get("question_text"),
                    }
                )

        return answers

    def _build_prompts(self, metadata: Mapping[str, Any]) -> List[Dict[str, Any]]:
        prompts_by_id: Mapping[str, Mapping[str, Any]] = metadata.get("prompts_by_id", {})
        prompts: List[Dict[str, Any]] = []
        for prompt_id, row in prompts_by_id.items():
            prompts.append(
                {
                    "id": prompt_id,
                    "prompt": self._extract_optional(
                        row, ["prompt", "name", "label", "text"]
                    )
                    or prompt_id,
                    "description": self._extract_optional(
                        row, ["desc", "description"]
                    ),
                    "control_type": self._extract_optional(
                        row, ["control_type", "controlType"]
                    ),
                    "created_at": row.get("created_at") or row.get("createdAt"),
                    "updated_at": row.get("updated_at") or row.get("updatedAt"),
                }
            )
        return prompts

    class ParsedAssignmentRow(NamedTuple):
        tagger_id: Optional[str]
        comment_id: str
        characteristic_id: str
        value: TagValue
        timestamp: datetime
        assignment_id: Optional[str]
        prompt_id: Optional[str]
        team_id: Optional[str]

    def _parse_assignment_fields(self, row: Mapping[str, Any]) -> ParsedAssignmentRow:
        tagger_id = self._extract_optional(row, ["tagger_id", "worker_id", "user_id"])
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

        assignment_id = self._extract_optional(
            row,
            ["assignment_id", "question_id", "questionId"],
        )
        prompt_id = self._extract_optional(row, ["prompt_id", "promptId"])
        team_id = self._extract_optional(row, ["team_id", "teamId"])

        return self.ParsedAssignmentRow(
            tagger_id=str(tagger_id) if tagger_id not in (None, "") else None,
            comment_id=comment_id,
            characteristic_id=characteristic_id,
            value=tag_value,
            timestamp=timestamp,
            assignment_id=str(assignment_id) if assignment_id not in (None, "") else None,
            prompt_id=str(prompt_id) if prompt_id not in (None, "") else None,
            team_id=str(team_id) if team_id not in (None, "") else None,
        )

    def _build_prompt_deployments(self, metadata: Mapping[str, Any]) -> List[Dict[str, Any]]:
        deployments_by_id: Mapping[str, Mapping[str, Any]] = metadata.get(
            "deployments_by_id", {}
        )
        prompts_by_id: Mapping[str, Mapping[str, Any]] = metadata.get("prompts_by_id", {})
        questions_by_id: Mapping[str, Mapping[str, Any]] = metadata.get(
            "questions_by_id", {}
        )

        deployments: List[Dict[str, Any]] = []
        for deployment_id, row in deployments_by_id.items():
            prompt_id = self._extract_optional(
                row, ["tag_prompt_id", "tagPromptId", "prompt_id", "promptId"]
            )
            question_id = self._extract_optional(
                row, ["assignment_id", "question_id", "questionId"]
            )
            questionnaire_id = self._extract_optional(
                row, ["questionnaire_id", "questionnaireId"]
            )
            question_type = self._extract_optional(
                row, ["question_type", "questionType"]
            )

            prompt_row = prompts_by_id.get(str(prompt_id)) if prompt_id not in (None, "") else None
            question_row = (
                questions_by_id.get(str(question_id))
                if question_id not in (None, "")
                else None
            )

            deployments.append(
                {
                    "id": deployment_id,
                    "prompt_id": str(prompt_id) if prompt_id not in (None, "") else None,
                    "question_id": str(question_id) if question_id not in (None, "") else None,
                    "questionnaire_id": str(questionnaire_id)
                    if questionnaire_id not in (None, "")
                    else None,
                    "question_type": str(question_type)
                    if question_type not in (None, "")
                    else None,
                    "prompt_label": self._extract_optional(
                        prompt_row or {}, ["prompt", "name", "label", "text"]
                    ),
                    "question_text": self._extract_optional(
                        question_row or {}, ["txt", "text", "question", "prompt"]
                    ),
                    "created_at": row.get("created_at") or row.get("createdAt"),
                    "updated_at": row.get("updated_at") or row.get("updatedAt"),
                }
            )
        return deployments

    def _build_questions(self, metadata: Mapping[str, Any]) -> List[Dict[str, Any]]:
        questions_by_id: Mapping[str, Mapping[str, Any]] = metadata.get(
            "questions_by_id", {}
        )
        questions: List[Dict[str, Any]] = []
        for question_id, row in questions_by_id.items():
            questions.append(
                {
                    "id": question_id,
                    "text": self._extract_optional(
                        row, ["txt", "text", "question", "prompt"]
                    )
                    or question_id,
                    "weight": row.get("weight"),
                    "questionnaire_id": row.get("questionnaire_id")
                    or row.get("questionnaireId"),
                    "sequence": row.get("seq") or row.get("sequence"),
                    "type": row.get("type"),
                    "max_label": row.get("max_label"),
                    "min_label": row.get("min_label"),
                    "alternatives": row.get("alternatives"),
                }
            )
        return questions

    def _record_assignment(
        self,
        *,
        assignment: TagAssignment,
        row: Mapping[str, Any],
        answers_lookup: Mapping[str, Mapping[str, Any]],
        deployments_lookup: Mapping[str, Mapping[str, Any]],
        prompts_lookup: Mapping[str, Mapping[str, Any]],
        questions_lookup: Mapping[str, Mapping[str, Any]],
        comment_meta: Dict[str, Dict[str, Any]],
        characteristic_meta: Dict[str, Dict[str, Any]],
        tagger_meta: Dict[str, Dict[str, Any]],
        assignments: List[TagAssignment],
        assignments_by_comment: DefaultDict[str, List[TagAssignment]],
        assignments_by_tagger: DefaultDict[str, List[TagAssignment]],
    ) -> None:
        comment_id = assignment.comment_id
        answer_row = answers_lookup.get(comment_id)
        comment_entry = comment_meta.get(comment_id)
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

        deployment_row = deployments_lookup.get(assignment.characteristic_id)
        deployment_prompt_id: Optional[Any] = None
        deployment_question_id: Optional[Any] = None
        deployment_questionnaire_id: Optional[Any] = None
        deployment_question_type: Optional[Any] = None

        if deployment_row:
            deployment_prompt_id = self._extract_optional(
                deployment_row,
                ["tag_prompt_id", "tagPromptId", "prompt_id", "promptId"],
            )
            deployment_question_id = self._extract_optional(
                deployment_row,
                ["assignment_id", "question_id", "questionId"],
            )
            deployment_questionnaire_id = self._extract_optional(
                deployment_row,
                ["questionnaire_id", "questionnaireId"],
            )
            deployment_question_type = self._extract_optional(
                deployment_row,
                ["question_type", "questionType"],
            )

        if prompt_id in (None, ""):
            prompt_id = self._extract_optional(row, ["prompt_id", "promptId"])
        if prompt_id in (None, ""):
            prompt_id = deployment_prompt_id
        if prompt_id in (None, ""):
            prompt_id = question_id or response_id or "unknown_prompt"

        if question_id in (None, "") and deployment_question_id not in (None, ""):
            question_id = deployment_question_id

        comment_meta_entry = comment_entry or {}
        comment_meta_entry.setdefault("text", str(comment_text))
        comment_meta_entry.setdefault(
            "prompt_id",
            str(prompt_id) if prompt_id not in (None, "") else "unknown_prompt",
        )
        comment_meta_entry.setdefault(
            "question_id", str(question_id) if question_id not in (None, "") else None
        )
        comment_meta_entry.setdefault(
            "response_id", str(response_id) if response_id not in (None, "") else None
        )
        comment_meta_entry.setdefault("answer_value", answer_value)
        if deployment_questionnaire_id not in (None, ""):
            comment_meta_entry.setdefault("questionnaire_id", str(deployment_questionnaire_id))
        if deployment_question_type not in (None, ""):
            comment_meta_entry.setdefault("question_type", str(deployment_question_type))
        question_row = None
        if question_id not in (None, ""):
            question_row = questions_lookup.get(str(question_id))
        if question_row:
            question_text = self._extract_optional(
                question_row,
                ["txt", "text", "question", "prompt"],
            )
            if question_text:
                comment_meta_entry.setdefault("question_text", str(question_text))
            questionnaire_from_question = self._extract_optional(
                question_row, ["questionnaire_id", "questionnaireId"]
            )
            if questionnaire_from_question not in (None, ""):
                comment_meta_entry.setdefault(
                    "questionnaire_id", str(questionnaire_from_question)
                )

        comment_meta[comment_id] = comment_meta_entry

        assignment_question_id = comment_meta_entry.get("question_id")
        assignment_questionnaire_id = comment_meta_entry.get("questionnaire_id")
        if assignment_question_id:
            assignment_question_id = str(assignment_question_id)
        if assignment_questionnaire_id:
            assignment_questionnaire_id = str(assignment_questionnaire_id)

        enriched_assignment = assignment
        if (
            assignment_question_id != getattr(assignment, "question_id", None)
            or assignment_questionnaire_id
            != getattr(assignment, "questionnaire_id", None)
        ):
            enriched_assignment = replace(
                assignment,
                question_id=assignment_question_id,
                questionnaire_id=assignment_questionnaire_id,
            )

        assignments.append(enriched_assignment)
        assignments_by_comment[assignment.comment_id].append(enriched_assignment)
        assignments_by_tagger[enriched_assignment.tagger_id].append(
            enriched_assignment
        )

        characteristic_id = assignment.characteristic_id
        char_entry = characteristic_meta.setdefault(characteristic_id, {})
        characteristic_name = char_entry.get("name")
        if not characteristic_name:
            characteristic_name = self._extract_optional(
                row,
                ["characteristic_name", "characteristic", "characteristicLabel", "name"],
            )
        if not characteristic_name and deployment_row:
            characteristic_name = self._extract_optional(deployment_row, ["name", "deployment_name"])
        if not characteristic_name:
            characteristic_name = comment_id

        prompt_row = None
        if deployment_prompt_id not in (None, ""):
            prompt_row = prompts_lookup.get(str(deployment_prompt_id))

        characteristic_prompt_id = char_entry.get("prompt_id")
        if not characteristic_prompt_id:
            characteristic_prompt_id = self._extract_optional(
                deployment_row or {},
                ["tag_prompt_id", "tagPromptId", "prompt_id", "promptId"],
            )
            if characteristic_prompt_id:
                char_entry.setdefault("prompt_id", str(characteristic_prompt_id))

        if prompt_row:
            prompt_label = self._extract_optional(
                prompt_row,
                ["prompt", "name", "label", "text"],
            )
            if prompt_label:
                characteristic_name = prompt_label
            prompt_description = self._extract_optional(
                prompt_row,
                ["desc", "description"],
            )
            if prompt_description and not char_entry.get("description"):
                char_entry["description"] = prompt_description
            control_type = self._extract_optional(
                prompt_row,
                ["control_type", "controlType"],
            )
            if control_type:
                char_entry.setdefault("control_type", str(control_type))

        characteristic_question_id = char_entry.get("question_id")
        if not characteristic_question_id:
            characteristic_question_id = self._extract_optional(
                deployment_row or {}, ["assignment_id", "question_id", "questionId"]
            )
            if characteristic_question_id:
                char_entry.setdefault("question_id", str(characteristic_question_id))

        characteristic_questionnaire_id = char_entry.get("questionnaire_id")
        if not characteristic_questionnaire_id:
            characteristic_questionnaire_id = self._extract_optional(
                deployment_row or {}, ["questionnaire_id", "questionnaireId"]
            )
            if characteristic_questionnaire_id:
                char_entry.setdefault("questionnaire_id", str(characteristic_questionnaire_id))

        deployment_question_type = (
            self._extract_optional(deployment_row or {}, ["question_type", "questionType"])
            if deployment_question_type in (None, "")
            else deployment_question_type
        )
        if deployment_question_type not in (None, ""):
            char_entry.setdefault("question_type", str(deployment_question_type))
        if question_row:
            question_text = self._extract_optional(
                question_row,
                ["txt", "text", "question", "prompt"],
            )
            if question_text:
                char_entry.setdefault("question_text", str(question_text))

        if not characteristic_name:
            characteristic_name = characteristic_id

        characteristic_description = char_entry.get("description")
        if not characteristic_description:
            characteristic_description = self._extract_optional(
                row,
                ["characteristic_description", "description"],
            )
            if characteristic_description:
                char_entry.setdefault("description", characteristic_description)

        char_entry.setdefault("name", str(characteristic_name))

        tagger_id_value = assignment.tagger_id
        tagger_entry = tagger_meta.setdefault(tagger_id_value, {})
        for key in ("team_id", "tagger_team", "tagger_meta"):
            if key in row and row[key] is not None:
                tagger_entry.setdefault(key, row[key])

    def _row_to_assignment(
        self,
        row: Mapping[str, Any],
        *,
        tagger_id_override: Optional[Any] = None,
        assignment_id_override: Optional[Any] = None,
    ) -> TagAssignment:
        parsed = self._parse_assignment_fields(row)

        tagger_id = tagger_id_override
        if tagger_id in (None, ""):
            tagger_id = parsed.tagger_id

        if tagger_id in (None, ""):
            raise KeyError(
                f"Missing required columns ['tagger_id', 'worker_id', 'user_id'] in row {row!r}"
            )

        assignment_id = assignment_id_override
        if assignment_id in (None, ""):
            assignment_id = parsed.assignment_id

        return TagAssignment(
            tagger_id=str(tagger_id),
            comment_id=parsed.comment_id,
            characteristic_id=parsed.characteristic_id,
            value=parsed.value,
            timestamp=parsed.timestamp,
            assignment_id=str(assignment_id) if assignment_id not in (None, "") else None,
            prompt_id=parsed.prompt_id,
            team_id=parsed.team_id,
        )

    _NUMERIC_TAG_VALUE_MAP = {
        0: TagValue.SKIP,
        1: TagValue.YES,
        2: TagValue.NA,
        3: TagValue.UNCERTAIN,
        4: TagValue.SKIP,
    }

    _NEGATIVE_TAG_VALUE_MAP = {
        -1: TagValue.NO,
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

