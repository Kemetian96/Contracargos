select
    t1.uid_orders,
    t1.id_orders_statuses
from main.t_orders t1
where t1.uid_orders in ({{orders_in}})
