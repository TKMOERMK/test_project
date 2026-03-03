## Блок 3 — Разбор чужого DAG'а (Airflow)
### Задания:

1. Найди потенциальные ошибки в DAG'е
2. Описать, как бы ты их исправил и почему
3. Добавить в код 1 дополнительный `task`, который проверяет, что таблица `orders_agg` не пуста (можно использовать `PostgresOperator` или `PythonOperator`)

Было:

```python
default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
```

Cтало:

```python
default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False, 
    'retries': 4, 
    'retry_delay': timedelta(minutes=4), 
}
```

Пояснения: 
1. `'depends_on_past': False,` : Поскольку у нас нестабильное соединение 'depends_on_past' лучше поставить на FALSE или убрать, тк он по дефолту FALSE, поскольку, если есть парамент 'depends_on_past' таска запустится, только если предыдущий экземпляр этой таски успешно завершился.
2. `'retries': 4, 'retry_delay': timedelta(minutes=4),` : Уменьшил количество ретраев и увеличил интервалы между ними, тк 5 раз через минуту это много быстроповторяющихся попыток, что может потом засорить логи при ошибке.

Было:

```python
dag = DAG(
    'order_processing_extended',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=True,
    tags=['orders'],
    max_active_runs=5
)
```
Стало:

```python
dag = DAG(
    dag_id='order_processing_extended',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False, #убираем true чтобы даг не собирал все данные с 2023 года по сегодняшний день, а начал с текущего времени, либо мы могли бы просто изменить datetime поставив сегоднящную дату (если нам это надо)
    tags=['orders'],
    max_active_runs=1, #тк соединение не стабильно ставим max_active_runs=1 иначе даг бы начал выполнение в следующий час пока не завершено выполнение предыдущего инстанса
    on_failure_callback=notify_failure # интегрировал
)
```
3.`dag_id='order_processing_extended',` :  Поменял на именнованный аргумент, чтобы читаемость была лучше, теперь мы понимает что это название айди дага.
4. `catchup=False,` : 









