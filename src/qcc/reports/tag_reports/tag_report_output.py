import csv
import math
from typing import Dict, List, Optional

import mysql.connector

from qcc.domain.tagassignment import TagAssignment
from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.io.db_adapter import DBAdapter
from qcc.reports.tag_reports.TagRecord import TagRecord
from qcc.reports.tagger_reports.tag_report import (
    group_by_comment_and_characteristic,
    alpha_for_item,
    kappa_for_item,
    calculate_tag_reliability,
    TagReportRow,
)
from qcc.domain.characteristic import Characteristic
from qcc.metrics.agreement import AgreementMetrics
from qcc.domain.enums import TagValue


class TagReportOutput:
    def db_to_csv(
        self,
        output_path: str,
        assignment_id: str,
        characteristic_id: Optional[str] = None,
    ):
        
        config = MySQLConfig(
            host="localhost",
            user="",
            password="",
            database="expertiza_anonymization",
            port=3306,
        )

        assignments = self.fetch_assignments_for_assignment(
            config=config,
            assignment_id=assignment_id,
            characteristic_id=characteristic_id,
        )

        self.write_to_csv(assignments, output_path)

    def fetch_assignments_for_assignment(
        self,
        config: MySQLConfig,
        assignment_id: str,
        characteristic_id: Optional[str] = None,
    ) -> List[TagAssignment]:
        sql = """
        SELECT
            at.user_id AS tagger_id,
            at.answer_id AS comment_id,
            at.tag_prompt_deployment_id AS characteristic_id,
            at.value AS value,
            COALESCE(at.updated_at, at.created_at) AS tagged_at,
            tpd.assignment_id AS assignment_id
        FROM answer_tags at
        JOIN tag_prompt_deployments tpd
            ON at.tag_prompt_deployment_id = tpd.id
        WHERE tpd.assignment_id = %s
        """

        params = [assignment_id]

        if characteristic_id is not None:
            sql += " AND at.tag_prompt_deployment_id = %s"
            params.append(characteristic_id)

        sql += """
        ORDER BY
            at.answer_id,
            at.tag_prompt_deployment_id,
            at.user_id
        """

        connection = mysql.connector.connect(**config.as_connector_kwargs())
        cursor = connection.cursor(dictionary=True)

        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        finally:
            cursor.close()
            connection.close()

        db_adapter = DBAdapter(mysql_config=config)
        assignments: List[TagAssignment] = []

        for row in rows:
            assignment = db_adapter._row_to_assignment(row)
            assignments.append(assignment)

        return assignments

    def build_records(self, assignments: List[TagAssignment]):
        grouped = group_by_comment_and_characteristic(assignments)

        records = []
        for (comment_id, char_id), tag_list in grouped.items():
            records.append(TagRecord(comment_id, char_id, tag_list))

        return records

    def _characteristic_assignments(
        self,
        assignments: List[TagAssignment],
    ) -> Dict[str, List[TagAssignment]]:
        result: Dict[str, List[TagAssignment]] = {}

        for assignment in assignments:
            char_id = str(getattr(assignment, "characteristic_id", ""))
            result.setdefault(char_id, []).append(assignment)

        return result

    def _build_per_tagger_metric_cache(
        self,
        assignments: List[TagAssignment],
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        by_characteristic = self._characteristic_assignments(assignments)
        metrics = AgreementMetrics()

        for characteristic_id, char_assignments in by_characteristic.items():
            characteristic = Characteristic(characteristic_id, characteristic_id)
            cache[characteristic_id] = metrics.per_tagger_metrics(
                char_assignments,
                characteristic,
                ("cohens_kappa", "krippendorffs_alpha"),
            )

        return cache

    def _normalize_agreement_to_reliability(self, value: float) -> float:
        normalized = (float(value) + 1.0) / 2.0
        return max(0.0, min(1.0, normalized))

    def _aggregate_tagger_reliability(
        self,
        record: TagRecord,
        per_tagger_metric_cache: Dict[str, Dict[str, Dict[str, float]]],
    ) -> Optional[float]:
        characteristic_metrics = per_tagger_metric_cache.get(record.characteristic_id, {})
        tagger_ids = {str(getattr(a, "tagger_id", "")) for a in record.assignments}

        per_tagger_scores: List[float] = []

        for tagger_id in tagger_ids:
            metric_map = characteristic_metrics.get(tagger_id, {})

            metric_values: List[float] = []
            for key in ("cohens_kappa", "krippendorffs_alpha"):
                value = metric_map.get(key)

                if isinstance(value, (int, float)) and math.isfinite(value):
                    normalized_value = self._normalize_agreement_to_reliability(value)
                    metric_values.append(normalized_value)

            if metric_values:
                per_tagger_score = sum(metric_values) / len(metric_values)
                per_tagger_scores.append(per_tagger_score)

        if not per_tagger_scores:
            return None

        return sum(per_tagger_scores) / len(per_tagger_scores)
    
    print("NEW AGGREGATE FUNCTION IS RUNNING")

    def build_rows(
        self,
        records: List[TagRecord],
        all_assignments: List[TagAssignment],
    ):
        rows = []
        per_tagger_metric_cache = self._build_per_tagger_metric_cache(all_assignments)

        for record in records:
            characteristic = Characteristic(
                record.characteristic_id,
                record.characteristic_id,
            )

            alpha = alpha_for_item(record.assignments, characteristic)
            kappa = kappa_for_item(record.assignments, characteristic)
            aggregate_reliability = self._aggregate_tagger_reliability(
                record,
                per_tagger_metric_cache,
            )
            
            tag_reliabilty_yes = calculate_tag_reliability(assignments=record.assignments, value=TagValue.YES)
            tag_reliabilty_no = calculate_tag_reliability(assignments=record.assignments, value=TagValue.NO)

            row = TagReportRow(
                comment_id=record.comment_id,
                characteristic_id=record.characteristic_id,
                num_taggers_could_set=record.num_taggers,
                num_yes=record.num_yes,
                num_no=record.num_no,
                num_skipped=record.num_skipped,
                cohens_kappa=kappa,
                krippendorffs_alpha=alpha,
                aggregate_tagger_reliability=aggregate_reliability,
                tag_reliability_yes=tag_reliabilty_yes,
                tag_reliability_no=tag_reliabilty_no
            )

            rows.append(row)

        return rows

    def write_to_csv(
        self,
        assignments: List[TagAssignment],
        output_path: str,
    ):

        records = self.build_records(assignments)
        rows = self.build_rows(records, assignments)

        headers = [
            "comment_id",
            "characteristic_id",
            "num_taggers_could_set",
            "num_yes",
            "num_no",
            "num_skipped",
            "cohens_kappa",
            "krippendorffs_alpha",
            "aggregate_tagger_reliability",
            "tag_reliability_yes",
            "tag_reliability_no",
        ]

        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for row in rows:
                writer.writerow(row.to_csv_row())

        print("CSV GENERATED")