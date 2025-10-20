"""Configuration helpers for connecting to a MySQL database."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class MySQLConfig:
    """Connection parameters required for accessing a MySQL database.

    Parameters
    ----------
    host:
        Hostname or IP address of the MySQL server.
    user:
        Username used to authenticate with the database.
    password:
        Password used to authenticate with the database.
    database:
        Name of the database to connect to.
    port:
        TCP port where the MySQL server is listening. Defaults to 3306.
    charset:
        Optional character set to use for the connection. UTF-8 is used by
        default when not provided.
    use_pure:
        When ``True``, ``mysql.connector`` will use the Python implementation
        instead of the C extension. Defaults to ``False`` to automatically pick
        the optimal implementation.
    """

    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    charset: Optional[str] = None
    use_pure: bool = False

    @classmethod
    def from_env(cls, prefix: str = "MYSQL") -> "MySQLConfig":
        """Build a configuration object using environment variables.

        Parameters
        ----------
        prefix:
            Prefix used in the environment variable names. For example, with
            the default prefix of ``MYSQL`` the variables ``MYSQL_HOST``,
            ``MYSQL_USER`` and so on are read.

        Returns
        -------
        MySQLConfig
            A populated configuration object.

        Raises
        ------
        ValueError
            If any required variables are missing.
        """

        env_map = {
            "host": os.getenv(f"{prefix}_HOST"),
            "user": os.getenv(f"{prefix}_USER"),
            "password": os.getenv(f"{prefix}_PASSWORD"),
            "database": os.getenv(f"{prefix}_DATABASE"),
            "port": os.getenv(f"{prefix}_PORT"),
            "charset": os.getenv(f"{prefix}_CHARSET"),
            "use_pure": os.getenv(f"{prefix}_USE_PURE"),
        }

        missing = [key for key in ("host", "user", "password", "database") if not env_map[key]]
        if missing:
            raise ValueError(
                "Missing required environment variables for MySQL configuration: "
                + ", ".join(f"{prefix}_{name.upper()}" for name in missing)
            )

        port_value = int(env_map["port"]) if env_map["port"] else 3306
        use_pure_value = (
            env_map["use_pure"].strip().lower() in {"1", "true", "yes"}
            if env_map["use_pure"]
            else False
        )

        return cls(
            host=env_map["host"],
            user=env_map["user"],
            password=env_map["password"],
            database=env_map["database"],
            port=port_value,
            charset=env_map["charset"],
            use_pure=use_pure_value,
        )

    @classmethod
    def from_dsn(cls, dsn: str) -> "MySQLConfig":
        """Build a configuration object from a MySQL DSN string.

        The DSN should follow the conventional format::

            mysql://user:password@host:port/database?charset=utf8mb4&use_pure=1

        Only the scheme, credentials, host, and database components are
        required. Query parameters are optional and currently recognise the
        ``charset`` and ``use_pure`` options.

        Parameters
        ----------
        dsn:
            Connection string describing how to connect to the database.

        Returns
        -------
        MySQLConfig
            Parsed configuration ready to be used with ``mysql.connector``.

        Raises
        ------
        ValueError
            If the DSN is missing required components or uses an unexpected
            scheme.
        """

        if not dsn:
            raise ValueError("MySQL DSN must be a non-empty string")

        parsed = urlparse(dsn)
        if parsed.scheme.lower() not in {"mysql", "mysql+mysqlconnector", "mysql+mysqldb"}:
            raise ValueError(f"Unsupported MySQL DSN scheme: {parsed.scheme!r}")

        if not parsed.username or not parsed.password:
            raise ValueError("MySQL DSN must include username and password")
        if not parsed.hostname:
            raise ValueError("MySQL DSN must include a hostname")

        # ``/database`` -> strip the leading slash
        database = parsed.path.lstrip("/") if parsed.path else ""
        if not database:
            raise ValueError("MySQL DSN must include a database name")

        query_params = parse_qs(parsed.query)

        charset = query_params.get("charset", [None])[0]
        use_pure_param = query_params.get("use_pure", [None])[0]
        use_pure = False
        if use_pure_param is not None:
            use_pure = str(use_pure_param).strip().lower() in {"1", "true", "yes"}

        port = parsed.port if parsed.port is not None else 3306

        return cls(
            host=parsed.hostname,
            user=parsed.username,
            password=parsed.password,
            database=database,
            port=port,
            charset=charset,
            use_pure=use_pure,
        )

    def as_connector_kwargs(self) -> Dict[str, object]:
        """Return keyword arguments compatible with ``mysql.connector.connect``."""

        kwargs: Dict[str, object] = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "port": self.port,
            "use_pure": self.use_pure,
        }
        if self.charset:
            kwargs["charset"] = self.charset
        return kwargs
