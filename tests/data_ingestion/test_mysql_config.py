import pytest

from qcc.data_ingestion import MySQLConfig


def test_from_dsn_parses_basic_components():
    config = MySQLConfig.from_dsn("mysql://user:pass@example.com:3307/database?charset=utf8mb4&use_pure=1")

    assert config.host == "example.com"
    assert config.user == "user"
    assert config.password == "pass"
    assert config.database == "database"
    assert config.port == 3307
    assert config.charset == "utf8mb4"
    assert config.use_pure is True


@pytest.mark.parametrize(
    "dsn",
    [
        "",
        "postgres://user:pass@host/db",
        "mysql://@host/db",
        "mysql://user@/db",
        "mysql://user:pass@host",
    ],
)
def test_from_dsn_invalid_inputs(dsn):
    with pytest.raises(ValueError):
        MySQLConfig.from_dsn(dsn)
