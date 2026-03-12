select
    t1.uid_orders,t1.total,
    t2.uid_rmas,t2.total
from main.t_orders t1
join main.t_rmas t2 on t1.id_orders = t2.id_orders
where t1.uid_orders in ({{orders_in}})
