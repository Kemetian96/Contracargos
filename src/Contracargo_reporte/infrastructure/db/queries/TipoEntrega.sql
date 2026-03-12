SELECT 
    t1.uid_orders,
    t2_top.id_orders_deliveries,
    t4.store,
    t5.eid_countries_ubigeos
FROM main.t_orders t1
-- Usamos LATERAL para buscar solo el primer registro relacionado
JOIN LATERAL (
    SELECT id_orders_deliveries, id_users_addresses_books
    FROM main.t_orders_shipments
    WHERE id_orders = t1.id_orders
    ORDER BY id_orders_deliveries ASC -- O la fecha de creación
    LIMIT 1
) t2_top ON TRUE
LEFT JOIN main.t_users_addresses_books t3 ON t2_top.id_users_addresses_books = t3.id_users_addresses_books
LEFT JOIN main.t_stores t4 ON t3.id_stores_deliveries = t4.id_stores
LEFT JOIN main.t_countries_ubigeos t5 ON t5.id_countries_ubigeos = t4.id_countries_ubigeos
WHERE t1.uid_orders IN ({{orders_in}})
