select
    t1.uid_orders,
    t1.total,
    t2.uid_rmas,
    t2.id_orders_used,
    t2.id_rmas_types,
    t2.total
from main.t_orders t1
left join main.t_rmas t2 on t1.id_orders = t2.id_orders
where t1.uid_orders in ({{orders_in}}) and id_rmas_statuses not in (-1,-2)
