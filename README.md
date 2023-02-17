# 概要
RedashからデータをAPI経由で取得するためのモジュール。

## 使い方

`pip install -r requirements.txt`

をした上で、

```
from redash import Redash
redash_credentials = '<<クレデンシャルファイルへのパス>>' # JSONフォーマットでAPIキーなどを保存。その保存先を指定。
redash = Redash(redash_credentials)

df = redash.query(1605) # query IDを数字で入れると、そのクエリ結果がpandasのDataFrameとして返ってくる。

# 行数が多い場合も、クエリに `limit_rows` と `offset_rows` というパラメータをつけてあげれば、指定した行数毎に
# 全てのデータを取得してくれる。
df = redash.safe_query(2674, params={'user_name':'John Doe', 'email':'johndoe@email.com'}, limit=100_000)

```