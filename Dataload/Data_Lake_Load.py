import pandas as pd
from datetime import datetime
import psycopg2

conn = psycopg2.connect("host='db' port='5432' dbname='database' user='username' password='secret'")
conn.autocommit = True
cur = conn.cursor()

df=pd.read_csv('Source_Data/source_data.csv',sep='\t')
print (df)

current_ts = datetime.now()
now = current_ts.strftime("%Y-%m-%d %H:%M:%S")
df["ETL_LOAD_TIME"] = now
print(df)
json = df.to_json('raw_data.json',orient='records',lines=True)

cur.execute('delete from DATALAKE.TRANSACTION_RAW_DATA src where src.etl_load_time::date=current_date')
copy_sql = """
           COPY DATALAKE.TRANSACTION_RAW_DATA (TRANSACTION_DATA) FROM stdin csv quote e'\x01' delimiter e'\x02'
           """
with open('raw_data.json', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)


dim_customer_update_sql="update TRANSACTIONS.DIM_CUSTOMER trg set CUSTOMER_FIRST_NAME=src.CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME=src.CUSTOMER_LAST_NAME from (with cte as (select transaction_data->>'First Name' as CUSTOMER_FIRST_NAME,transaction_data->>'Last Name' as CUSTOMER_LAST_NAME,transaction_data->>'SSN' as CUSTOMER_SSN,row_number() over(partition by transaction_data->>'SSN' order by transaction_data->>'First Name',transaction_data->>'Last Name' asc) as rn  from  datalake.transaction_raw_data where ETL_LOAD_TIME::date=current_date)select * from cte where rn=1) src where src.CUSTOMER_SSN=trg.CUSTOMER_SSN"
cur.execute(dim_customer_update_sql)
dim_customer_insert_sql="insert into TRANSACTIONS.DIM_CUSTOMER (CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME,CUSTOMER_SSN) select src.CUSTOMER_FIRST_NAME,src.CUSTOMER_LAST_NAME,src.CUSTOMER_SSN from (with cte as (select transaction_data->>'First Name' as CUSTOMER_FIRST_NAME,transaction_data->>'Last Name' as CUSTOMER_LAST_NAME,transaction_data->>'SSN' as CUSTOMER_SSN,row_number() over(partition by transaction_data->>'SSN' order by transaction_data->>'First Name',transaction_data->>'Last Name' asc) as rn  from  datalake.transaction_raw_data where ETL_LOAD_TIME::date=current_date)select * from cte where rn=1) src left join TRANSACTIONS.DIM_CUSTOMER trg on src.CUSTOMER_SSN=trg.CUSTOMER_SSN where trg.CUSTOMER_SSN is null;"
cur.execute(dim_customer_insert_sql)

dim_transaction_type_insert_sql="insert into TRANSACTIONS.DIM_TRANSACTION_TYPE (TRANSACTION_TYPE_NAME) select src.TRANSACTION_TYPE_NAME from (select distinct transaction_data->>'Type' as TRANSACTION_TYPE_NAME from  datalake.transaction_raw_data where transaction_data->>'Type' is not null and etl_load_time::date=current_date) src left join TRANSACTIONS.DIM_TRANSACTION_TYPE trg on src.TRANSACTION_TYPE_NAME=trg.TRANSACTION_TYPE_NAME where trg.TRANSACTION_TYPE_NAME is null;"
cur.execute(dim_transaction_type_insert_sql)


fact_transaction_update_sql="update TRANSACTIONS.FACT_TRANSACTION trg set TRANSACTION_DATE=src.TRANSACTION_DATE,TRANSACTION_TYPE=src.TRANSACTION_TYPE,CUSTOMER_ID=src.CUSTOMER_ID,TERMS=src.TERMS,TOTAL_AMOUNT=src.TOTAL_AMOUNT from " \
                            "(select SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID,TO_DATE(transaction_data->>'Date','yyyy-mm-dd') as TRANSACTION_DATE,type.TRANSACTION_TYPE_ID as TRANSACTION_TYPE,cust.CUSTOMER_ID as CUSTOMER_ID,transaction_data->>'Terms' as TERMS,TO_NUMBER(transaction_data->>'Amount','L9G999g999.99') as TOTAL_AMOUNT from datalake.transaction_raw_data src left join TRANSACTIONS.DIM_CUSTOMER as cust on cust.customer_ssn=src.transaction_data->>'SSN' left join TRANSACTIONS.DIM_TRANSACTION_TYPE type on type.TRANSACTION_TYPE_NAME=src.transaction_data->>'Type' where src.etl_load_time::date=current_date) src where src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID "

cur.execute(fact_transaction_update_sql)

fact_transaction_insert_sql="insert into TRANSACTIONS.FACT_TRANSACTION (TRANSACTION_ID,SUB_TRANSACTION_ID,TRANSACTION_DATE,TRANSACTION_TYPE,CUSTOMER_ID,TERMS,TOTAL_AMOUNT) select src.TRANSACTION_ID,src.SUB_TRANSACTION_ID,src.TRANSACTION_DATE,src.TRANSACTION_TYPE,src.CUSTOMER_ID,src.TERMS,src.TOTAL_AMOUNT from (select SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID,TO_DATE(transaction_data->>'Date','yyyy-mm-dd') as TRANSACTION_DATE,type.TRANSACTION_TYPE_ID as TRANSACTION_TYPE,cust.CUSTOMER_ID as CUSTOMER_ID,transaction_data->>'Terms' as TERMS,TO_NUMBER(transaction_data->>'Amount','L9G999g999.99') as TOTAL_AMOUNT from datalake.transaction_raw_data src left join TRANSACTIONS.DIM_CUSTOMER as cust on cust.customer_ssn=src.transaction_data->>'SSN' left join TRANSACTIONS.DIM_TRANSACTION_TYPE type on type.TRANSACTION_TYPE_NAME=src.transaction_data->>'Type' where src.etl_load_time::date=current_date) src left join TRANSACTIONS.FACT_TRANSACTION trg on src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID where trg.TRANSACTION_ID is null and trg.SUB_TRANSACTION_ID is null "
cur.execute(fact_transaction_insert_sql)

fact_transaction_line_item_update_sql="update TRANSACTIONS.FACT_TRANSACTION_LINE_ITEMS trg set PRODUCT_NAME=src.PRODUCT_NAME,UNIT_NAME=src.UNIT_NAME,TOTAL_QUANTITY=src.TOTAL_QUANTITY from (with cte as (select REPLACE(transaction_data->>'LineItems'::text,'\','')::jsonb as transaction_data,SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID from datalake.transaction_raw_data where etl_load_time::date=current_date)select distinct cte.transaction_id,cte.sub_transaction_id, id as TRANSACTION_LINE_ITEM_ID,name as PRODUCT_NAME,unit as UNIT_NAME,quantity as TOTAL_QUANTITY from cte,jsonb_to_recordset(cte.transaction_data) as x(\"id\" int, \"name\" text,\"unit\" text,\"quantity\" int)) src where src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID and src.TRANSACTION_LINE_ITEM_ID=trg.TRANSACTION_LINE_ITEM_ID"
cur.execute(fact_transaction_line_item_update_sql)

fact_transaction_line_item_insert_sql="insert into TRANSACTIONS.FACT_TRANSACTION_LINE_ITEMS (TRANSACTION_ID,SUB_TRANSACTION_ID,TRANSACTION_LINE_ITEM_ID,PRODUCT_NAME,UNIT_NAME,TOTAL_QUANTITY) select src.TRANSACTION_ID,src.SUB_TRANSACTION_ID,src.TRANSACTION_LINE_ITEM_ID,src.PRODUCT_NAME,src.UNIT_NAME,src.TOTAL_QUANTITY from (with cte as (select REPLACE(transaction_data->>'LineItems'::text,'\','')::jsonb as transaction_data,SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID from datalake.transaction_raw_data where etl_load_time::date=current_date) select distinct cte.transaction_id,cte.sub_transaction_id, id as TRANSACTION_LINE_ITEM_ID,name as PRODUCT_NAME,unit as UNIT_NAME,quantity as TOTAL_QUANTITY from cte,jsonb_to_recordset(cte.transaction_data) as x(\"id\" int, \"name\" text,\"unit\" text,\"quantity\" int)) src left join TRANSACTIONS.FACT_TRANSACTION_LINE_ITEMS trg on src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID and src.TRANSACTION_LINE_ITEM_ID=trg.TRANSACTION_LINE_ITEM_ID where trg.TRANSACTION_ID is null and trg.SUB_TRANSACTION_ID is null and trg.TRANSACTION_LINE_ITEM_ID is null"
cur.execute(fact_transaction_line_item_insert_sql)