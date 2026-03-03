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
    'retries': 4, #уменишли
    'retry_delay': timedelta(minutes=4), #увеличили
}
```

Пояснения: 
1. 'depends_on_past': False,`  нестабильное соединение 'depends_on_past' лучше поставить на фалсе или убрать тк он по дефолту FALSE тк если есть парамент 'depends_on_past' таска запустится только если предыдущий экземпляр этой таски успешно завершился
