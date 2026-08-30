# Multi-Tenant Activity Report

Halcyon Analytics hosts product analytics for hundreds of customer workspaces. Every night the collectors flush one Parquet drop containing raw events for every workspace, and a morning job turns that drop into the activity report each customer sees on their dashboard.

Read the event feed from `INPUT_PATH` and write one row per `tenant_id` as Parquet to `OUTPUT_PATH`:

- `event_count` — how many events the tenant produced
- `distinct_users` — number of distinct `user_id` values
- `distinct_sessions` — number of distinct `session_id` values
- `total_amount` — sum of `amount`, as `DECIMAL(14,2)`

The feed has seven columns: `event_id`, `tenant_id`, `event_date`, `user_id`, `session_id`, `action`, and `amount`. Only the four figures above belong in the report.

Traffic is nowhere near evenly spread. Halcyon's busiest workspace produces more events than every other workspace combined, and a workspace is a single group — one key, handled by one task. The executor you are given is deliberately small: one core and 512 MB of heap, the same slot this job gets in the nightly schedule.

**Note: the busiest workspace does not fit in the executor heap. Any approach that first gathers one tenant's ids into a single collection will fall over at full scale.**
