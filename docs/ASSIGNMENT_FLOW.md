# Assignment ingestion and tagger attachment flow

This note outlines how assignment rows from the database become domain objects
and how they are attached to taggers for reporting.

## Building assignments from database tables
1. **Import rows** – The database adapter (`DBAdapter`) loads the configured
   tables and passes the assignment table rows into `_build_assignments`.
2. **Normalize lookups** – Within `_build_assignments`, the adapter indexes
   supporting tables (`answers`, `assignment_questionnaires`,
   `tag_prompt_deployments`, `tag_prompts`, and `questions`) so assignment-level
   metadata can be enriched.
3. **Parse each row** – `_parse_assignment_fields` extracts the required
   identifiers (`comment_id`, `characteristic_id`), the tag value, timestamp,
   any provided prompt or team IDs, and a `tagger_id` when present. Final
   `TagAssignment` objects are instantiated after enrichment so missing tagger
   IDs can be filled.
4. **Override assignment IDs and fill missing taggers from authoritative tables** –
   When an `assignment_questionnaires` row matches the assignment's
   `tagger_id`, its `assignment_id` becomes the assignment identifier. If no
   questionnaire row exists, a matching `tag_prompt_deployments` row (keyed by
   the `characteristic_id`) contributes its `assignment_id`/`question_id`
   value; the authoritative ID replaces any value from the assignment row. If
   an assignment ID is available but the row omitted a `tagger_id`, the
   adapter back-fills the tagger using the `assignment_questionnaires` mapping
   keyed by `assignment_id` before creating the `TagAssignment`. Rows missing
   both a tagger ID and a corresponding `assignment_questionnaires` entry
   cannot be ingested and will raise a `KeyError` so the missing ownership can
   be corrected upstream.
5. **Collect metadata** – As assignments are appended, the adapter groups them by
   comment and tagger, and records comment/characteristic/tagger metadata to
   support later object construction.

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
