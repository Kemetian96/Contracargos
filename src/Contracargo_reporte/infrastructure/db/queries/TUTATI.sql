-- Query PostgreSQL. Usa parametros CUID: %(fecha1)s y %(fecha2)s.
SELECT 
    t1.eid_orders,
    t1.cuid_documented,
    t1.total,
    t1.uid_orders
FROM main.t_orders t1
JOIN main.t_stores t2 
    ON t2.id_stores = t1.id_stores_documented
WHERE 
  t1.cuid_documented      >=  %(fecha1)s
   AND t1.cuid_documented <  %(fecha2)s
   and t1.id_orders_statuses <> ALL (ARRAY[-1,-2,-3])
  AND t1.id_orders_types <> 3
  AND t1.eid_orders NOT LIKE '%%V%%'
  AND t2.id_commerces = 1;
