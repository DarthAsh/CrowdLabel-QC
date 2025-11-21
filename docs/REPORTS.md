# Reporting reference

This document summarizes what each reporting surface in QCC produces and how
its metrics are calculated. Use it as a quick reference when interpreting
`summary.json` output or CSV exports produced by the CLI. CSV exports are
written as timestamped files (e.g., `tagging-report-20240620-153045.csv`).

## Data inputs and object model

Reports consume the domain objects built by the MySQL importer. Assignment rows
from the first configured table (typically `answer_tags`) are enriched via
`answers` → `questions` → `assignment_questionnaires` to recover the
authoritative `assignment_id`, with `tag_prompt_deployments` supplying a
secondary assignment ID when needed. Rows missing a tagger are rejected during
parsing. The resulting `TagAssignment` objects (plus comments, taggers,
characteristics, prompts, and questions) are what the report classes read.

## Tagger performance report

`TaggerPerformanceReport` bundles three families of metrics: tagging speed,
pattern detection, and agreement (optional). The report accepts the full set of
`Tagger` instances and the `TagAssignment` list they carry.

For per-assignment visibility into pattern detection, see
`PatternDetectionReport` below.

### Speed metrics

Speed is calculated by the `LogTrimTaggingSpeed` strategy. For each tagger the
strategy:

1. Collects only assignments with a timestamp (taggers with fewer than two
   timestamped assignments are skipped).
2. Sorts those assignments by timestamp and builds the interval—in seconds—
   between each consecutive pair.
3. Discards any non-positive intervals and converts the rest to `log2` seconds.
4. Trims the longest 10% of intervals by count (upper tail) to reduce the
   impact of long pauses or idle sessions.
5. Returns the mean of the remaining `log2` intervals as `mean_log2`.
6. Converts `mean_log2` back to a friendlier value using
   `seconds_per_tag = 2 ** mean_log2`.

The summary section lists the strategy name (`"LogTrimTaggingSpeed"`), per
-tagger `mean_log2` values, the derived `seconds_per_tag`, and the number of
`timestamped_assignments` included for each tagger.

### Pattern detection

Pattern detection looks for repeating yes/no sequences that might signal
copy-paste behavior or inattentive labeling.

* **Input sequence** – All timestamped `YES`/`NO` tag assignments for a tagger,
  sorted chronologically, are converted to a token string such as `"YYNNYY"`.
* **Search window** – The analyzer inspects non-overlapping windows of 12
  assignments to find strict repeats. A window counts as a hit only when the
  entire 12-token substring is a perfect repetition of its first 4 tokens
  (`abcdabcdabcd`) or first 3 tokens (`abcabcabcabc`). Other mixes are ignored.
* **Masking** – Windows attributed to a 4-token repeat are masked out before
  the search for 3-token repeats to avoid double-counting the same region.
* **Patterns reported** – Counts are recorded against the literal pattern that
  filled the 3- or 4-token repeat window. In practice the most common hits are
  the uniform sequences `YYYY` and `NNNN`, but any 3–4 token pattern qualifies
  if it fills the entire window.

The report also emits the list of tracked patterns from
`PatternCollection.return_all_patterns()` so downstream consumers know the
canonical ordering.

### Agreement metrics

Agreement is delegated to `qcc.metrics.agreement.AgreementMetrics` and uses the
latest-label semantics from that module. When enabled, the report computes any
combination of:

* `percent_agreement`
* `cohens_kappa`
* `krippendorffs_alpha`
* `agreement_matrix`

Results are grouped by characteristic. Per-tagger sub-metrics (when available)
are also collected so they can be averaged into the CSV export.

### CSV export layout

`TaggerPerformanceReport.export_to_csv` flattens the JSON summary into a
columnar CSV. Column prefixes indicate the source of each metric:

* `speed_*` – Speed strategy name and per-tagger values (`mean_log2`,
  `seconds_per_tag`, `timestamped_assignments`).
* `pattern_*` – Pattern strategy names and per-pattern counts. Both horizontal
  and vertical sections use the same prefix with an optional `_horizontal` or
  `_vertical` infix when both are present.
* `agreement_*` – Averaged per-tagger agreement values when the agreement
  section is included.

Each row represents one tagger (`user_id`). Columns are added only when the
corresponding metric exists in the summary payload, so the CSV remains compact
for partial reports.

## Assignment pattern detection report

`PatternDetectionReport` produces assignment-level outputs for pattern detection
so you can trace the signals back to individual assignments. Pattern detection
always runs within each tagger's individual assignment (all answer tags sharing
an `assignment_id`). Only assignment `1205` is emitted in the report. In
addition to the detected patterns, each assignment row reports what percentage
of its timestamped YES/NO tags belong to a detected pattern window, how many
tags land inside patterns, the total tags examined, how many answers were
tagged in the assignment, how many answer tags were present overall, and the
tagging speed metrics for that assignment.

The report returns a single entry for every tagger/assignment pair with the
assignment identifiers and pattern(s) found when scanning that assignment's
answer tags. Patterns are detected in 12-assignment windows using the same 3-
and 4-token repeat logic as `TaggerPerformanceReport`.

CSV exports include one row per assignment with a semicolon-delimited
`detected_patterns` column (empty when no patterns were detected) and a boolean
`has_repeating_pattern` column for quick filtering. Only `tagger_id`,
`assignment_id`, first-comment/prompt/timestamp metadata, tag and answer counts,
and the pattern/speed columns are emitted.

### How patterns are detected and attached

- **Input preparation:** For each tagger, assignments are grouped by
  `assignment_id`. Within each group only timestamped YES/NO tags are kept,
  sorted chronologically, and converted into token strings for scanning.
- **Window scan:** Non-overlapping 12-assignment windows within the assignment
  are inspected. A window is a hit only when its tokens are a perfect
  repetition of the first four tokens (`abcdabcdabcd`) or first three tokens
  (`abcabcabcabc`). Windows matching four-token repeats are masked before three-
  token matching to avoid double-counting.
- **Annotation:** When a window hits, the assignment is marked with the literal
  repeated pattern (for example, `YYYY`, `NNNN`, or `YYNN`), and
  `has_repeating_pattern` is set to `true` for that tagger/assignment pair. An
  assignment can accrue multiple pattern labels if multiple windows match.

### CSV column reference

- `tagger_id`, `assignment_id` – the tagger and assignment identifiers; only
  assignment `1205` rows are written.
- `# Tags Available` – maximum possible tags for the tagger/assignment pair,
  summed once per distinct answered comment. For each comment, its
  `question_id` is resolved to a questionnaire, and only questionnaires `753`
  (2 tags per answer) and `754` (1 tag per answer) contribute to this total;
  answers tied to other questionnaires are ignored. Skipped answers still
  contribute to this availability total when their questionnaire is counted,
  even though they are ineligible for pattern detection.
- `# Tags Set` – number of eligible timestamped YES/NO tags examined for the
  tagger/assignment pair (the input to pattern detection).
- `# Tags Set in a pattern` – count of those eligible tags that fell within at
  least one detected pattern window.
- `# Comments available to tag` – number of distinct answers (comment IDs)
  tagged by the user for the assignment.
- `detected_patterns` – semicolon-delimited list of patterns that hit within the
  assignment; empty when no pattern was detected for that row.
- `has_repeating_pattern` – `true` when any pattern was found for that
  assignment, else `false`.
- `pattern_coverage_pct` – percentage (0–100, rounded to two decimal places) of
  the assignment's eligible tags that fell inside one or more detected pattern
  windows.
- `trimmed_seconds_per_tag` – seconds-per-tag using the log-trimmed mean
  computed from the assignment's eligible tags.

Use the CSV export to trace any flagged pattern back to the exact assignment and
context that produced it.

## Characteristic reliability report

`CharacteristicReliabilityReport` is scaffolded but unimplemented. The
class-level docstrings describe the intended surface for prevalence and
agreement analysis once those methods are filled in.
