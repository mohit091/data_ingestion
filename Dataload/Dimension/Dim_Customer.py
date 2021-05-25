from  Utils import connection
import traceback


class dim_customer_load():

    def data_load(self,host,port,dbname,user,password):
        try:
            host=host
            port=port
            dbname=dbname
            user=user
            password=password
            conn_details=connection.database_connection.connection(host,port,dbname,user,password)
            conn=conn_details[0]
            cur=conn_details[1]
            dim_customer_update_sql = "update TRANSACTIONS.DIM_CUSTOMER trg set CUSTOMER_FIRST_NAME=src.CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME=src.CUSTOMER_LAST_NAME from (with cte as (select transaction_data->>'First Name' as CUSTOMER_FIRST_NAME,transaction_data->>'Last Name' as CUSTOMER_LAST_NAME,transaction_data->>'SSN' as CUSTOMER_SSN,row_number() over(partition by transaction_data->>'SSN' order by transaction_data->>'First Name',transaction_data->>'Last Name' asc) as rn  from  datalake.transaction_raw_data where ETL_LOAD_TIME::date=current_date)select * from cte where rn=1) src where src.CUSTOMER_SSN=trg.CUSTOMER_SSN"
            cur.execute(dim_customer_update_sql)
            print ('DIM_CUSTOMER UPDATED SUCCESSFULLY')
            dim_customer_insert_sql = "insert into TRANSACTIONS.DIM_CUSTOMER (CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME,CUSTOMER_SSN) select src.CUSTOMER_FIRST_NAME,src.CUSTOMER_LAST_NAME,src.CUSTOMER_SSN from (with cte as (select transaction_data->>'First Name' as CUSTOMER_FIRST_NAME,transaction_data->>'Last Name' as CUSTOMER_LAST_NAME,transaction_data->>'SSN' as CUSTOMER_SSN,row_number() over(partition by transaction_data->>'SSN' order by transaction_data->>'First Name',transaction_data->>'Last Name' asc) as rn  from  datalake.transaction_raw_data where ETL_LOAD_TIME::date=current_date)select * from cte where rn=1) src left join TRANSACTIONS.DIM_CUSTOMER trg on src.CUSTOMER_SSN=trg.CUSTOMER_SSN where trg.CUSTOMER_SSN is null;"
            cur.execute(dim_customer_insert_sql)
            print('DIM_CUSTOMER inserted SUCCESSFULLY')
            cur.close()
            conn.close()
            print('DIM_CUSTOMER LOADED SUCCESSFULLY')
        except:
            errMsg = traceback.format_exc()
            print('Not able to load  data to dim_customer because of following error ' + errMsg)
