import pandas as pd

df = pd.read_csv('user_actions.csv')

df = pd.DataFrame(df)

#смотрим первые 10 строк
print(df.head(10))

#смотрим типы колонок
print(df.dtypes)

#тк event_time у нас object приведем его в datetime
df['event_time'] = pd.to_datetime(df['event_time'])

#посмотрим сколько пустых значений в датафрейме, как видим таковых нет
print(df.isna().sum())

# создаем колонки
df['views'] = (df['event_type'] == 'view').astype(int)
df['clicks'] = (df['event_type'] == 'click').astype(int)
df['purchases'] = (df['event_type'] == 'purchase').astype(int)

#группируем и получаем просмотры, клики и покупки, а также мин. и макс. время события
df = df.groupby("user_id").agg(
    views=pd.NamedAgg(column="views", aggfunc="sum"),
    clicks=pd.NamedAgg(column="clicks", aggfunc="sum"),
    purchases=pd.NamedAgg(column="purchases", aggfunc="sum"),
    first_event=pd.NamedAgg(column="event_time", aggfunc="min"),
    last_event=pd.NamedAgg(column="event_time", aggfunc="max"),
    count=pd.NamedAgg(column="event_time", aggfunc="count"),
)

# тк мы перевернули таблицу у нас user_id стал индексом, поэтому надо обнулить индекс
df = df.reset_index()

#вычитаем из последнего события первое и делим на количество интервалов (количество событий - 1)
df['avg_interval_sec'] = (df['last_event'] - df['first_event']).dt.total_seconds() / (df['count'] - 1)

print(df[['user_id', 'views', 'clicks', 'purchases', 'avg_interval_sec', 'first_event', 'last_event']])

df[['user_id', 'views', 'clicks', 'purchases', 'avg_interval_sec', 'first_event', 'last_event']].to_csv("user_report.csv", index=False)
