"""Configuration schema for QCC."""

from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class InputConfig(BaseModel):
    """Input configuration settings."""

    format: str = Field(default="csv", description="Input format (csv, mysql)")
    path: Optional[str] = Field(
        default=None,
        description="Path to input file when using the CSV input format",
    )
    mysql: "MySQLInputConfig" = Field(
        default_factory=lambda: MySQLInputConfig(),
        description="MySQL connection settings when using the mysql input format",
    )


class MySQLInputConfig(BaseModel):
    """MySQL-specific connection configuration."""

    host: Optional[str] = Field(default=None, description="MySQL server hostname")
    port: Optional[int] = Field(default=None, description="MySQL server port")
    user: Optional[str] = Field(default=None, description="MySQL user name")
    password: Optional[str] = Field(default=None, description="MySQL user password")
    database: Optional[str] = Field(default=None, description="Database name")
    charset: Optional[str] = Field(default=None, description="Connection character set")
    use_pure: bool = Field(
        default=False,
        description="Force mysql.connector to use the pure Python implementation",
    )
    env_prefix: str = Field(
        default="MYSQL",
        description="Environment variable prefix used to fill missing MySQL settings",
    )
    dsn: Optional[str] = Field(
        default=None,
        description="Optional DSN string (mysql://user:pass@host:port/db) overriding other values",
    )


class OutputConfig(BaseModel):
    """Output configuration settings."""
    
    directory: str = Field(default="reports", description="Output directory")
    format: str = Field(default="json", description="Output format (json, html, csv)")


class AgreementConfig(BaseModel):
    """Agreement analysis configuration."""
    
    methods: List[str] = Field(
        default=["percent_agreement", "cohens_kappa", "krippendorffs_alpha"],
        description="Agreement methods to use"
    )
    min_agreement: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Minimum acceptable agreement score"
    )


class SpeedConfig(BaseModel):
    """Speed analysis configuration."""
    
    enabled: bool = Field(default=True, description="Enable speed analysis")
    min_interval_seconds: float = Field(
        default=1.0,
        ge=0.0,
        description="Minimum interval between assignments (seconds)"
    )
    max_interval_seconds: float = Field(
        default=3600.0,
        ge=0.0,
        description="Maximum interval between assignments (seconds)"
    )


class PatternsConfig(BaseModel):
    """Pattern detection configuration."""
    
    enabled: bool = Field(default=True, description="Enable pattern detection")
    min_repetition_count: int = Field(
        default=3,
        ge=1,
        description="Minimum repetitions to consider a pattern"
    )
    max_pattern_length: int = Field(
        default=10,
        ge=1,
        description="Maximum pattern length to detect"
    )


class AnalysisConfig(BaseModel):
    """Analysis configuration settings."""
    
    agreement: AgreementConfig = Field(default_factory=AgreementConfig)
    speed: SpeedConfig = Field(default_factory=SpeedConfig)
    patterns: PatternsConfig = Field(default_factory=PatternsConfig)


class CharacteristicReliabilityConfig(BaseModel):
    """Characteristic reliability reporting configuration."""
    
    enabled: bool = Field(default=True, description="Enable characteristic reliability reporting")
    include_prevalence: bool = Field(default=True, description="Include prevalence analysis")
    include_agreement: bool = Field(default=True, description="Include agreement analysis")


class TaggerPerformanceConfig(BaseModel):
    """Tagger performance reporting configuration."""
    
    enabled: bool = Field(default=True, description="Enable tagger performance reporting")
    include_speed: bool = Field(default=True, description="Include speed analysis")
    include_patterns: bool = Field(default=True, description="Include pattern analysis")
    include_agreement: bool = Field(default=True, description="Include agreement analysis")


class ReportingConfig(BaseModel):
    """Reporting configuration settings."""
    
    characteristic_reliability: CharacteristicReliabilityConfig = Field(
        default_factory=CharacteristicReliabilityConfig
    )
    tagger_performance: TaggerPerformanceConfig = Field(
        default_factory=TaggerPerformanceConfig
    )


class LoggingConfig(BaseModel):
    """Logging configuration settings."""

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log message format"
    )
    file: Optional[str] = Field(
        default="qcc.log",
        description=(
            "Log file name or path (relative paths are resolved within the output directory)"
        ),
    )


class QCCConfig(BaseModel):
    """Main QCC configuration schema."""
    
    input: InputConfig = Field(default_factory=InputConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"
        validate_assignment = True

