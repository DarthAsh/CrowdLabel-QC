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
   `input.format` is `mysql`) and an output directory for the generated tagging
   report.

   **Command:**

   ```bash
   PYTHONPATH=src python -m qcc.cli.main run \
     --config path/to/mysql_config.yml \
     --in ignored.csv \
     --out path/to/output_dir
   ```

The command connects to MySQL with the configured credentials, imports the
crowd-labeling tables, and writes `summary.json` plus a timestamped
`tagging-report-YYYYMMDD-HHMMSS.csv` under the chosen output directory.

### Minimal MySQL setup steps

Follow these steps to spin up the CLI against a MySQL database from scratch:

1. **Install MySQL.** Use your platform’s package manager or the official
   installers to provision a local MySQL server.

2. **Import the SQL dump.** Load your exported labeling tables into MySQL
   (for example, `mysql -u <user> -p <database> < dump.sql`). Confirm the
   import succeeds and that the resulting tables match the columns expected
   by your QCC configuration (task assignments, tags, timestamps, etc.).

3. **Update the config for your connection.** Open your YAML config (for
   example, `src/qcc/config/default.yml`) and set the `input.mysql` values to
   match your host, port, user, password, and database name. If the dump uses
   a non-default schema or table names, align the `input.mysql.tables`
   mappings accordingly so the CLI reads from the correct tables.

4. **Run the tagging report.** Execute the CLI, pointing to your config and an
   output directory for the generated artifacts. On Windows, for example:

   ```bash
   python -m qcc.cli.main run --config "D:\\Independent Study\\CrowdLabel-QC\\src\\qcc\\config\\default.yml" --in ignored.csv --out <your directory path>
   ```

The CLI will connect with your configured credentials, read the imported
MySQL data, and emit the JSON summary plus the timestamped `tagging-report-*.csv`
into the target output directory.
To run use the command:
python -m qcc.cli.main run --config "<your directory>\\CrowdLabel-QC\src\qcc\config\default.yml" --in ignored.csv --out <output directory>

## Reports and metrics

See [`docs/REPORTS.md`](docs/REPORTS.md) for a detailed description of the
reports produced by QCC, including how tagging speed, pattern detection, and
agreement metrics are calculated and exported.

## Development


```bash

```
![QC](https://github.com/user-attachments/assets/8d38b948-00ca-43de-a23b-affc47a76883)
## License
