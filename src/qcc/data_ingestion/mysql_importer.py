"""MySQL data import helpers."""

from __future__ import annotations

from contextlib import contextmanager
import re
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from .mysql_config import MySQLConfig

DEFAULT_TAG_PROMPT_TABLES: Sequence[str] = (
    "answer_tags",
    "answers",
    "tag_prompt_deployments",
    "tag_prompts",
    "questions",
    "assignment_questionnaires",
)


@contextmanager
def mysql_connection(config: MySQLConfig):
    """Context manager that yields an open MySQL connection."""

    try:
        import mysql.connector  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        message = (
            "mysql-connector-python is required to use the MySQL input adapter. "
            "Install it with `pip install mysql-connector-python` or provide an "
            "alternative driver that exposes the `mysql.connector` module."
        )
        raise ModuleNotFoundError(message) from exc

    connection = mysql.connector.connect(**config.as_connector_kwargs())
    try:
        yield connection
    finally:
        connection.close()


class TableImporter:
    """Utility class that reads rows from MySQL tables into dictionaries."""

    def __init__(self, config: MySQLConfig):
        self._config = config

    def fetch_table(self, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return the full contents of a table as a list of dictionaries.

        Parameters
        ----------
        table_name:
            The table to read from. The name is validated to contain only
            alphanumeric characters and underscores to prevent SQL injection.
        limit:
            Optionally restrict the number of rows returned from the table.
        """

        if not table_name or not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
            raise ValueError(f"Invalid table name: {table_name!r}")

        query = f"SELECT * FROM `{table_name}`"
        params: Optional[Sequence[object]] = None
        if limit is not None:
            limit_value = int(limit)
            if limit_value < 0:
                raise ValueError("limit must be non-negative")
            query += " LIMIT %s"
            params = (limit_value,)

        with mysql_connection(self._config) as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()

        return rows

    def import_tables(
        self,
        table_names: Iterable[str],
        limit: Optional[int] = None,
    ) -> Mapping[str, List[Dict[str, Any]]]:
        """Fetch multiple tables and return a mapping of table name to rows."""

        data: MutableMapping[str, List[Dict[str, Any]]] = {}
        for name in table_names:
            data[name] = self.fetch_table(name, limit=limit)
        return data


def import_tag_prompt_deployment_tables(
    config: MySQLConfig,
    tables: Sequence[str] = DEFAULT_TAG_PROMPT_TABLES,
    limit: Optional[int] = None,
) -> Mapping[str, List[Dict[str, Any]]]:
    """Import tag prompt deployment data from the configured MySQL database.

    The default ``tables`` value matches the schema shared in the quality control
    dashboards: a table that stores answers and a table that stores confidence
    levels. The function can be customised by passing a different sequence of
    table names.
    """

    importer = TableImporter(config)
    return importer.import_tables(tables, limit=limit)

# # src/qcc/data_ingestion/mysql_importer.py
# import mysql.connector
# from contextlib import contextmanager

# class TableImporter:
#     def __init__(self, config: MySQLConfig):
#         self.config = config

#     @contextmanager
#     def _conn(self):
#         cnx = mysql.connector.connect(**self.config.as_connector_kwargs())
#         try:
#             yield cnx
#         finally:
#             cnx.close()

#     def import_tables(self, tables, limit=None):
#         results = {}
#         with self._conn() as cnx:
#             cur = cnx.cursor(dictionary=True)
#             for t in tables:
#                 q = f"SELECT * FROM `{t}`"
#                 if limit:
#                     q += f" LIMIT {int(limit)}"
#                 cur.execute(q)
#                 results[t] = [dict(row) for row in cur.fetchall()]
#             cur.close()
#         return results

# def import_tag_prompt_deployments(config: MySQLConfig, tables=DEFAULT_TAG_PROMPT_TABLES, limit=None):
#     importer = TableImporter(config)
#     return importer.import_tables(tables, limit=limit)

