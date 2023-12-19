from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np

MYSQL_CONNECTION = "mysql_default" # ชื่อ connector ใน Airflow ที่ตั้งค่าไว้

# path ทั้งหมดที่จะใช้
customer_raw_output_path = "/home/airflow/gcs/data/raw/customer.csv"
product_raw_output_path = "/home/airflow/gcs/data/raw/product.csv"
transaction_raw_output_path = "/home/airflow/gcs/data/raw/transaction.csv"
customer_cleaned_output_path = "/home/airflow/gcs/data/cleaned/customer.csv"
final_output_path = "/home/airflow/gcs/data/cleaned/final_output.csv"


def get_data_from_database(customer_raw_path, product_raw_path, transaction_raw_path):
    # รับ customer_raw_path, roduct_raw_path, transaction_raw_path มาจาก task ที่เรียกใช้

    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # Query จาก database โดยใช้ Hook ที่สร้าง ได้ผลลัพธ์เป็น pandas DataFrame
    customer_raw = mysqlserver.get_pandas_df(sql = "SELECT * FROM customer")
    product_raw = mysqlserver.get_pandas_df(sql = "SELECT * FROM product")
    transaction_raw = mysqlserver.get_pandas_df(sql = "SELECT * FROM transaction")

    # Save เป็น csv
    customer_raw.to_csv(customer_raw_path, index = False)
    print(f"Output to {customer_raw_path}")

    product_raw.to_csv(product_raw_path, index = False)
    print(f"Output to {product_raw_path}")

    transaction_raw.to_csv(transaction_raw_path, index = False)
    print(f"Output to {transaction_raw_path}")


def clear_null_in_customer_table (customer_raw_path, customer_cleaned_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    customer = pd.read_csv(customer_raw_path)

    # แทนที่ค่า null ด้วยค่าเฉลี่ย
    mean_age = np.mean(customer.age)
    customer["age"].fillna(mean_age.round(1), inplace = True)

    # Save เป็น csv ไฟล์ไปที่ customer_cleaned_path ("/home/airflow/gcs/data/cleaned/customer.csv")
    customer.to_csv(customer_cleaned_path, index = False)
    print(f"Output to {customer_cleaned_path}")


def merge_data(customer_cleaned_path, product_raw_path, transaction_raw_path, final_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    customer = pd.read_csv(customer_cleaned_path)
    product = pd.read_csv(product_raw_path)
    transaction = pd.read_csv(transaction_raw_path)

    # แปลง unit_price ใน product โดยเอาเครื่องหมาย $ ออก และแปลงให้เป็น float
    product["unit_price"] = product.apply(lambda x: x["unit_price"].replace("$",""), axis = 1)
    product["unit_price"] = product["unit_price"].astype(float)

    # merge 3 DataFrame
    final = transaction.merge(product, how = "left", left_on = "product_id", right_on = "product_id").merge(customer, how = "left", left_on = "customer_id", right_on = "customer_id")

    # เพิ่ม column total_amount
    final["total_amount"] = final["quantity"] * final["unit_price"]

    # Save เป็น csv ไฟล์ไปที่ final_path ("/home/airflow/gcs/data/cleaned/final_output.csv")
    final.to_csv(final_path, index = False)
    print(f"Output to {final_path}")


with DAG(
    "coffee_shop_transaction_to_bq",
    start_date = days_ago(1),
    schedule_interval = "@once",
    tags = ["coffee_shop_transaction"]
) as dag:

    t1 = PythonOperator (
        task_id = "get_data_from_database",
        python_callable = get_data_from_database,
        op_kwargs = {
            "customer_raw_path" : customer_raw_output_path,
            "product_raw_path" : product_raw_output_path,
            "transaction_raw_path" : transaction_raw_output_path
        }
    )

    t2 = PythonOperator (
        task_id = "clear_null_in_customer_table",
        python_callable = clear_null_in_customer_table,
        op_kwargs = {
            "customer_raw_path" : customer_raw_output_path,
            "customer_cleaned_path" : customer_cleaned_output_path
        }
    )

    t3 = PythonOperator (
        task_id = "merge_data",
        python_callable = merge_data,
        op_kwargs = {
            "customer_cleaned_path" : customer_cleaned_output_path,
            "product_raw_path" : product_raw_output_path,
            "transaction_raw_path" : transaction_raw_output_path,
            "final_path" : final_output_path
        }
    )

    t4 = BashOperator (
        task_id = "load_to_bq",
        bash_command = "bq load \
            --source_format=CSV \
            --autodetect \
            coffee_shop.coffee_shop_transaction \
            gs://us-central1-project1-5439c9c3-bucket/data/cleaned/final_output.csv"
    )

    #กำหนด dependencies ให้แต่ละ tasks
    t1 >> t2 >> t3 >> t4