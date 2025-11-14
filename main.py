from flask import Flask, request, jsonify, render_template
from google.cloud import bigquery

app = Flask(__name__)

# --- BigQuery 設定 ---
project_id = "bigquery-477702"
dataset_id = "bq_sam"
# table_id is now dynamic
client = bigquery.Client(project=project_id)

def generate_bigquery_query(project_id: str, dataset_id: str, table_id: str,
                            select_columns: str = "*", where_clause: str = None, limit: int = None) -> str:
    """
    Args:
        project_id (str): 專案 ID。
        dataset_id (str): 資料集 ID。
        table_id (str): 資料表 ID。
        select_columns (str, optional): 欲選取的欄位名稱 (如 "name, age")。預設為 "*" (所有欄位)。
        where_clause (str, optional): 查詢條件 (例如 "age > 18 AND country = 'TW'")。
        limit (int, optional): 限制回傳的資料筆數。
    Returns:
        str: 完整的 SQL 查詢語法字串。
    """

    path = f"`{project_id}.{dataset_id}.{table_id}`"

    sql_query = f"""
SELECT
    {select_columns}
FROM
    {path}
"""
    # 處理 WHERE 條件
    if where_clause:
        sql_query += f"\nWHERE\n    {where_clause}"

    # 處理 LIMIT 限制
    if limit is not None and limit > 0:
        sql_query += f"\nLIMIT {limit}"
        

    return sql_query.strip() + ";" # .strip() 移除開頭多餘的空行，並加上分號

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/query', methods=['POST'])
def query_bigquery():
    data = request.get_json()
    print(data)
    query_type = data.get('type')
    query_id = data.get('id')

    if not query_type:
        return jsonify({"error": "缺少 'type' 參數"}), 400

    if query_type == 'client':
        table_id = 'client'
        id_column = 'client_id'
    elif query_type == 'employee':
        table_id = 'emploee'
        id_column = 'emp_id'
    else:
        return jsonify({"error": "無效的查詢類型"}), 400

    where_clause = None
    # 只有在 query_id 有值時才建立 WHERE 條件
    if query_id: 
        try:
            if(query_id.isdigit()):
                int(query_id)
                where_clause = f"{id_column} = {query_id}"  # 如果 id 是數值類型，則不應加單引號
            #如果不是數值，則是輸入名稱，更改欄位
            else:
                print("string input")
                if table_id == "client":
                    id_column = "client_name"
                elif table_id == "emploee":
                    id_column = "name"
                where_clause = f"{id_column}='{query_id}'"
                print(where_clause)
        except ValueError:
            return jsonify({"error": "條件指定錯誤"}), 400

    QUERY = generate_bigquery_query(
        project_id, 
        dataset_id, 
        table_id,
        select_columns="*", 
        where_clause=where_clause,
        limit=10
    )

    try:
        query_job = client.query(QUERY)
        results = query_job.result()

        # 將結果轉換為 list of dictionaries
        records = [dict(row) for row in results]
        
        return jsonify(records)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
