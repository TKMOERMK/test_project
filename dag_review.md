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
    catchup=False, 
    tags=['orders'],
    max_active_runs=1, 
    on_failure_callback=notify_failure 
)
```

3. `dag_id='order_processing_extended',` :  Поменял на именнованный аргумент, чтобы читаемость была лучше, теперь мы понимает что это название айди дага.
4. `catchup=False,` : Убираю true чтобы даг не собирал все данные с 2023 года по сегодняшний день, а начал с текущего времени, либо мы могли бы просто изменить datetime поставив сегоднящную дату (если нам это надо).
5. ` max_active_runs=1,` : Поскольку соединение нестабильно ставим max_active_runs=1 иначе даг бы начал выполнение в следующий час, пока не завершено выполнение предыдущего инстанса.
6. `on_failure_callback=notify_failure` : notify_failure объявлен но не интегрирован, поэтому я его интегрировал в dag on_failure_callback=notify_failure.

Было:

```python
from airflow.operators.dummy import DummyOperator
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)
```

Стало: 

```python
from airflow.operators.empty import EmptyOperator
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)
```

7. Заменил `DummyOperator` на `EmptyOperator`, поскольку это более современный подход.

Было:
```python
def extract_orders(**kwargs):
    print("Extracting orders...")
    if random.random() < 0.2:
        raise Exception("Random extraction failure")  

def transform_orders(**kwargs):
    print("Transforming orders...")

def load_orders(**kwargs):
    print("Loading orders to warehouse...")

def notify_failure(context):
    print("Sending alert to Slack...") 

extract = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    provide_context=True,
    dag=dag,
)
```

Стало:
```python
def extract_orders(**context):
    print("Extracting orders...")
    if random.random() < 0.2:
        raise Exception("Random extraction failure")  

def transform_orders(**context):
    print("Transforming orders...")

def load_orders(**context):
    print("Loading orders to warehouse...")

def notify_failure(**context): 
    print("Sending alert to Slack...") 

extract = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

load = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=dag,
)
```

8. Убрал везде `provide_context`, поскольку он устарел и соотестветственно `**kwargs` заменил на `**contex`

Было:
```python
check_not_empty = PostgresOperator(
    task_id='check_agg_table',
    postgres_conn_id='postgres_default',
    sql='SELECT COUNT(*) FROM orders_agg;',
    dag=dag,
)
```
Стало: 

```python
from airflow.hooks.postgres_hook import PostgresHook

def check_orders_not_empty():
    hook = PostgresHook(postgres_conn_id='postgresELMIR')
    result = hook.get_first("SELECT COUNT(*) FROM orders_agg;")

    if result[0] == 0:
        raise ValueError("Таблица пуста\(")

check_not_empty = PythonOperator(
    task_id='check_agg_table',
    python_callable=check_orders_not_empty,
    dag=dag,
)
```
9. Изначальная таска не делает проверку, она возвращает число, причем даже если там 0 он все равно не упадет, что бесмысленно, тк мы буквально проверяем на пустоту, поэтому в соответствие с заданием : 3. Добавить в код 1 дополнительный `task`, который проверяет, что таблица `orders_agg` не пуста (можно использовать `PostgresOperator` или `PythonOperator`) я добавил таск используя хук.







