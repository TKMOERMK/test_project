# Тестовое задание : Блок 1 — SQL анализ и валидация данных

## Задания:
1. Найди пользователей, у которых расхождение между `orders_agg.total_orders` и реальным количеством заказов в `orders_raw`
### Вариант 1

```sql
select 
	r.user_id,
	COUNT(r.order_id) as raw_total_orders,
	MAX(a.total_orders) as agg_total_orders
from orders_raw r
full outer join orders_agg a 
on r.user_id = a.user_id
group by r.user_id
having COUNT(r.order_id)!=MAX(a.total_orders)
order by r.user_id 
```

Пояснение: 
Мы берём всех пользователей из двух таблиц сразу (FULL OUTER JOIN), причем даже если пользователь есть только в одной таблице, он всё равно попадёт в результат.
Для каждого пользователя считаем:
- raw_total_orders = сколько заказов реально есть в сырой таблице 
- agg_total_orders = сколько заказов записано в сводной таблице
  
Далее мы группируем всё по пользователю и оставляем только тех, у кого реальное количество заказов не равно тому, что написано в агрегированной таблице.
Сортируем по user_id.
