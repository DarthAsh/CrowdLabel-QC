# Assignment ingestion and tagger attachment flow

This note outlines how assignment rows from the database become domain objects
and how they are attached to taggers for reporting.

## Building assignments from database tables
1. **Import rows** – The database adapter (`DBAdapter`) reads the tables listed
   in `input.mysql.tables`; the first entry is always treated as the assignments
   table (for example, `answer_tags`). All rows from that table are passed into
   `_build_assignments`.
2. **Normalize lookups** – Within `_build_assignments`, the adapter pre-indexes
   supporting tables (`answers`, `assignment_questionnaires`,
   `tag_prompt_deployments`, `tag_prompts`, and `questions`) so enrichment data
   is ready before any assignment row is parsed.
3. **Parse each row** – `_parse_assignment_fields` extracts the required
   identifiers (`comment_id` sourced from `comment_id`/`item_id`/`answer_id`,
   `characteristic_id`, and `tagger_id`), the tag value (numeric `1/-1/0` map to
   YES/NO/SKIP), timestamp, and any provided prompt or team IDs. Rows without a
   tagger/worker/user raise a `KeyError` immediately. Parsed fields are carried
   forward to the enrichment step before a `TagAssignment` is created.
4. **Resolve assignment IDs via questionnaire linkage** – For every parsed row,
   the adapter follows the chain `comment_id (answer_id) → answers.question_id →
   questions.questionnaire_id → assignment_questionnaires.assignment_id` to find
   the authoritative assignment identifier. If that chain produces a value, it
   overwrites any ID from the assignment row. When the questionnaire lookup is
   empty, the adapter falls back to the `tag_prompt_deployments` row keyed by the
   `characteristic_id`, then to any row-level ID, and finally marks the ID as
   missing.
5. **Backfill empty answers** – After all rows are parsed, any answer from the
   `answers` table that did not receive a tag row is given a synthetic
   `TagAssignment` with a SKIP (numeric `0`) value. The adapter uses the
   questionnaire mapping to supply the user/assignment IDs and the deployment
   lookup (via `question_id`) to pick the characteristic, skipping only when a
   tagger or characteristic cannot be resolved.
6. **Collect metadata** – As assignments are appended, the adapter groups them by
   comment and tagger, and records comment/characteristic/tagger metadata (plus
   ID-resolution statistics and a sample wiring log) to support later object
   construction.

## Attaching assignments to taggers
1. **Build taggers** – After assignments are built, `_build_taggers` walks the
   grouped `assignments_by_tagger` map to instantiate each `Tagger` with its
   corresponding list of `TagAssignment` objects.
2. **Preserve tagger metadata** – Any tagger-level fields captured during
   `_build_assignments` (for example, `team_id`) are forwarded as the `meta`
   payload when each `Tagger` is created.

The resulting taggers carry all timestamped assignments (with deployment-backed
assignment IDs) that downstream reports consume for speed, pattern detection,
and agreement calculations.
