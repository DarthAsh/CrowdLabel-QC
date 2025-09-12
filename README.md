# QCC - Quality Control of Crowd labeling

A Python library for analyzing and reporting on crowd labeling quality metrics.

## Features

- **Agreement Metrics**: Calculate inter-annotator agreement using various methods
- **Speed Analysis**: Analyze tagging speed patterns and identify potential issues
- **Pattern Detection**: Detect repeated patterns that may indicate systematic bias
- **Comprehensive Reporting**: Generate detailed reports on characteristic reliability and tagger performance

## Installation

```bash
pip install qcc
```

## Quick Start

```bash
# Run quality control analysis
qcc run --in data/assignments.csv --out reports/ --config config/default.yml
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/ tests/
ruff check src/ tests/

# Run pre-commit hooks
pre-commit run --all-files
```

## License

MIT License - see LICENSE file for details.
