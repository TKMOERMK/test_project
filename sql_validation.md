# Тестовое задание : Блок 1 — SQL анализ и валидация данных


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

```python 
print("Hello") ```








Мы берём всех пользователей из двух таблиц сразу (FULL OUTER JOIN).
Даже если пользователь есть только в одной таблице — он всё равно попадёт в результат.
Для каждого пользователя считаем:
raw_total_orders = сколько заказов реально есть в сырой таблице (orders_raw)
agg_total_orders = сколько заказов записано в сводной таблице (orders_agg)

GROUP BY r.user_id — мы группируем всё по пользователю.
HAVING COUNT(...) != MAX(...) — оставляем только тех, у кого реальное количество заказов не равно тому, что написано в агрегированной таблице.
Сортируем по user_id.
