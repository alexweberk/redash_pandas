from redash import Redash

# creds_fileを使う場合
creds_file = "somefolder/path_to_creds_file"
redash = Redash(creds_file)

# クエリを実行する
df = redash.query(2600)

# 行数の多すぎるクエリを実行する場合には、safe_queryを使う
df2 = redash.safe_query(2880,
                        params={'station1': '池袋', 'station2': '新宿'},
                        limit=100_000)
