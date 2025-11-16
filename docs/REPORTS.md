# Reporting reference

This document summarizes what each reporting surface in QCC produces and how
its metrics are calculated. Use it as a quick reference when interpreting
`summary.json` output or CSV exports produced by the CLI.

## Tagger performance report

`TaggerPerformanceReport` bundles three families of metrics: tagging speed,
pattern detection, and agreement (optional). The report accepts the full set of
`Tagger` instances and the `TagAssignment` list they carry.

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

Two perspectives are provided:

* `horizontal`: analyzes the full chronological sequence for each tagger.
* `vertical`: aggregates patterns within each characteristic separately and
  merges the counts for a tagger.

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

## Characteristic reliability report

`CharacteristicReliabilityReport` is scaffolded but unimplemented. The
class-level docstrings describe the intended surface for prevalence and
agreement analysis once those methods are filled in.
