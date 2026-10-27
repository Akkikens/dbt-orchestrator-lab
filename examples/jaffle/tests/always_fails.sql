-- Singular test that always returns rows (fails). Disabled unless a job passes
-- --vars '{enable_failing_test: true}', so normal builds never select it; the
-- orchestrator's failure and retry paths use it to hit a real dbt failure.
{{ config(tags=['always_fails'], enabled=var('enable_failing_test', false)) }}

select customer_id from {{ ref('customers') }} where order_count >= 0
