from flask import Flask, request, jsonify, render_template
from google.cloud import bigquery

app = Flask(__name__)

# --- BigQuery 設定 ---
project_id = "bigquery-477702"
dataset_id = "bq_sam"
client = bigquery.Client(project=project_id)

#檢查是否為query起始(query需不需要加AND)
def check_querystart(query_start) ->bool:
    if(query_start):
        return True
    else:
        return False

def set_wherequery(column_name:str , search: str | int , start: bool)->str:
    return_str=""
    if not start:
        return_str += f" AND\n"

    if search.isdigit() and column_name != "phone":
        return_str += f"{column_name} = {search}"
    else:
        return_str += f"{column_name}='{search}'"
    return return_str
        

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
    query_branchid = data.get('branch_id')
    query_salary = data.get('salary')
    query_sex= data.get('sex')
    query_supid=data.get('sup_id')
    query_phone = data.get('phone')

    #計算有幾個where條件查詢 扣掉選擇client或employee(此為選擇table)

    if query_type == 'client':
        table_id = 'client'
    elif query_type == 'employee':
        table_id = 'emploee'
        #id_column = 'emp_id'

    query_start = False

    where_clause = ""

 # 只有在有值時才建立 WHERE 條件
    if query_id: 
        try:
            query_start = True
            if query_id.isdigit():
                int(query_id)
                if table_id == "client":
                    where_clause += set_wherequery("client_id",query_id,True)
                elif table_id == "emploee":
                    where_clause += set_wherequery("emp_id",query_id,True)
            #如果不是數值，則是輸入名稱，更改欄位
            else:
                if table_id == "client":
                    where_clause += set_wherequery("client_name",query_id,True)
                elif table_id == "emploee":
                    where_clause+= set_wherequery("name",query_id,True)
        except ValueError:
            return jsonify({"error": "id 或 name 條件指定錯誤"}), 400

    if query_branchid:
        if not query_branchid.isdigit() :
            return jsonify({"error": "branch id 條件指定錯誤"}), 400
        else:
            int(query_branchid)
            if check_querystart(query_start) == False:
                query_start=True;
                where_clause += set_wherequery("branch_id",query_branchid,True)
            else:
                where_clause += set_wherequery("branch_id",query_branchid,False)
        

    if query_salary:
        if not query_salary.isdigit() :
                return jsonify({"error": "salary 條件指定錯誤"}), 400
        else:
            int(query_salary)
            if check_querystart(query_start) == False:
                query_start=True;
                where_clause += set_wherequery("salary",query_salary,True)
            else:
                where_clause += set_wherequery("salary",query_salary,False)

    if query_sex:
        if query_sex.isdigit():
            return jsonify({"error": "sex 條件指定錯誤"}), 400
        else:
            if check_querystart(query_start)== False :
                query_start=True;
                where_clause += set_wherequery("sex",query_sex,True)
            else:
                where_clause += set_wherequery("sex",query_sex,False)

    if  query_supid:
        if not  query_supid.isdigit() :
                return jsonify({"error": "supervisor id 條件指定錯誤"}), 400
        else:
            int(query_supid)
            if check_querystart(query_start) == False:
                query_start=True;
                where_clause += set_wherequery("sup_id",query_supid,True)
            else:
                where_clause += set_wherequery("sup_id",query_supid,False)

    if  query_phone:
        if not  query_phone.isdigit() :
                return jsonify({"error": "phone 條件指定錯誤"}), 400
        else:
            if check_querystart(query_start) == False:
                query_start=True;
                where_clause += set_wherequery("phone",query_phone,True)
            else:
                where_clause += set_wherequery("phone",query_phone,False)

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
