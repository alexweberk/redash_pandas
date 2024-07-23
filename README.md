# 概要

Redash からデータを API 経由で取得するためのモジュール。

## 使い方

### セットアップ

Colab などで使う場合：

```
!pip install git+https://github.com/alexweberk/redash_pandas.git
```

をした上で、

```
from redash_pandas import Redash

# クレデンシャルファイルを使う場合
redash_credentials = '<<クレデンシャルファイルへのパス>>' # JSONフォーマットでAPIキーなどを保存。その保存先を指定。
redash = Redash(credentials=redash_credentials)
```

もしくは、

```
# api_keyとendpointを直接指定する場合
redash = Redash(
    apikey="YOUR_API_KEY",
    endpoint="https://redash.example.com"
)
```

## クエリの仕方

### クエリの負荷が大きくない場合

```
df = redash.query(42) # query IDを数字で入れると、そのクエリ結果がpandasのDataFrameとして返ってくる。
```

### クエリの負荷が大きい場合

```
# 行数が多い場合も、クエリ内に `limit_rows` と `offset_rows` というパラメータをつけてあげれば、指定した行数毎に
# 全てのデータを取得してくれる。
df = redash.safe_query(2674, params={'user_name':'John Doe', 'email':'johndoe@email.com'}, limit=100_000)
```

### クエリ取得を期間で絞れる場合

```
# 2023-01-01から2024-06-20までのデータに関して、３ヶ月単位でデータを取得する
df = redash.period_limited_query(6738, start_date='2023-01-01', end_date='2024-06-20',
            interval='month', interval_multiple = 3)

# 2023-01-01から2024-06-20までのデータに関して、４週間単位でデータを取得する
df = redash.period_limited_query(6738, start_date='2023-01-01', end_date='2024-06-20',
            interval='week', interval_multiple = 4)
```

クエリ側では、下記のような書き方をする必要がある。

```
select
    date_trunc('month', bookings.created_at + interval '9 hours') b_mo
    , count(distinct bookings.id) b_cnt
    , sum(bookings.price) b_price
from bookings
where true
    and bookings.status = 1
    and bookings.created_at + interval '9 hours' between '{{start_date}}'::date 
        and '{{end_date}}'::date - interval '1 second'
group by 1
order by 1
```
