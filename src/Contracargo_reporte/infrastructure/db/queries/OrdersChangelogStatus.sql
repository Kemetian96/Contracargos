DROP TABLE IF EXISTS tt_orders_changelogs;
CREATE TEMP TABLE tt_orders_changelogs AS
SELECT
    t1.id_orders,
    t2.uid_orders,
    t1.changelog,
    t1.cuid_inserted AS uuid_short_modified
FROM main.t_orders_changelogs t1
LEFT JOIN main.t_orders t2 ON t1.id_orders = t2.id_orders
WHERE t2.uid_orders IN ({{orders_in}});

SELECT
    id_orders,
    uid_orders,
    (changelog::jsonb ->> 'comment') AS comment,
    (changelog::jsonb ->> 'id_orders_statuses')::INT AS id_orders_statuses,
    (changelog::jsonb ->> 'id_users_updated')::BIGINT AS id_users_updated,
    (changelog::jsonb ->> 'cuid_updated')::BIGINT AS cuid_updated
FROM tt_orders_changelogs
WHERE (changelog::jsonb ->> 'comment') LIKE 'Estado orden:%'
ORDER BY uid_orders, cuid_updated;
