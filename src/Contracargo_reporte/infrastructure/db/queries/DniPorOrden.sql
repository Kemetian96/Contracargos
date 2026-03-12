select
    t1.uid_orders,
    t3.document
from main.t_orders t1
left join main.t_users t3 on t1.id_users = t3.id_users
where t1.uid_orders in ({{orders_in}})
