from redash import Redash

# creds_fileを使う場合
creds_file = "somefolder/path_to_creds.json"
redash = Redash(creds_file)

# endpoint, apikeyを直接使う場合
redash = Redash(apikey="<<YOUR_API_KEY>>", endpoint="https://<<YOUR_ENDPOINT>>")

# クエリを実行する
df = redash.query(2600)

# クエリをする際にパラメータが存在する時
# {"パラメータ名": "代入する値"}
params={"station1": "池袋", "station2": "新宿"}
query_id = 200
df = redash.query(query_id, params)

# 行数の多すぎるクエリを実行する場合には、safe_queryを使う
# クエリのパラメータには必ず{{offset_rows}}と{{limit_rows}}の二つのパラメータを用意する
df2 = redash.safe_query(2880, limit=100_000)
