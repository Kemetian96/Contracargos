select
    t1.uid_orders,
    t2.id_users_egiftcards_statuses
from main.t_orders t1
join main.t_users_egiftcards t2 on t1.id_orders = t2.id_orders
where t1.uid_orders in ({{orders_in}})
