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

Run the CLI directly from a source checkout (no package install required) by
pointing `PYTHONPATH` at the `src/` tree:

```bash
PYTHONPATH=src python -m qcc.cli.main --help
```

### Using a MySQL input source

1. Copy the default configuration and adjust the `input` block for MySQL:

   ```yaml
   input:
     format: mysql
     mysql:
       host: db.example.com
       port: 3306
       user: qc_reader
       password: s3cret
       database: crowd_quality
   ```

2. Run the CLI, choosing any placeholder path for `--in` (it is ignored when
   `input.format` is `mysql`) and an output directory for the generated summary.

   **Command:**

   ```bash
   PYTHONPATH=src python -m qcc.cli.main run \
     --config path/to/mysql_config.yml \
     --in ignored.csv \
     --out path/to/output_dir
   ```

The command connects to MySQL with the configured credentials, imports the
crowd-labeling tables, and writes `summary.json` plus a timestamped
`summary-YYYYMMDD-HHMMSS.csv` under the chosen output directory.

## Reports and metrics

See [`docs/REPORTS.md`](docs/REPORTS.md) for a detailed description of the
reports produced by QCC, including how tagging speed, pattern detection, and
agreement metrics are calculated and exported.

## Development


```bash

```
![QC](https://github.com/user-attachments/assets/8d38b948-00ca-43de-a23b-affc47a76883)
## License
