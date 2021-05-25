from Utils import connection
import traceback


class fact_transaction_load():

    def data_load(self, host, port, dbname, user, password):
        try:
            host = host
            port = port
            dbname = dbname
            user = user
            password = password
            conn_details = connection.database_connection.connection(host, port, dbname, user, password)
            conn = conn_details[0]
            cur = conn_details[1]
            fact_transaction_update_sql = "update TRANSACTIONS.FACT_TRANSACTION trg set TRANSACTION_DATE=src.TRANSACTION_DATE,TRANSACTION_TYPE=src.TRANSACTION_TYPE,CUSTOMER_ID=src.CUSTOMER_ID,TERMS=src.TERMS,TOTAL_AMOUNT=src.TOTAL_AMOUNT from " \
                                          "(select SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID,TO_DATE(transaction_data->>'Date','yyyy-mm-dd') as TRANSACTION_DATE,type.TRANSACTION_TYPE_ID as TRANSACTION_TYPE,cust.CUSTOMER_ID as CUSTOMER_ID,transaction_data->>'Terms' as TERMS,TO_NUMBER(transaction_data->>'Amount','L9G999g999.99') as TOTAL_AMOUNT from datalake.transaction_raw_data src left join TRANSACTIONS.DIM_CUSTOMER as cust on cust.customer_ssn=src.transaction_data->>'SSN' left join TRANSACTIONS.DIM_TRANSACTION_TYPE type on type.TRANSACTION_TYPE_NAME=src.transaction_data->>'Type' where src.etl_load_time::date=current_date) src where src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID "

            cur.execute(fact_transaction_update_sql)
            print('FACT_TRANSACTION UPDATED SUCCESSFULLY')

            fact_transaction_insert_sql = "insert into TRANSACTIONS.FACT_TRANSACTION (TRANSACTION_ID,SUB_TRANSACTION_ID,TRANSACTION_DATE,TRANSACTION_TYPE,CUSTOMER_ID,TERMS,TOTAL_AMOUNT) select src.TRANSACTION_ID,src.SUB_TRANSACTION_ID,src.TRANSACTION_DATE,src.TRANSACTION_TYPE,src.CUSTOMER_ID,src.TERMS,src.TOTAL_AMOUNT from (select SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID,TO_DATE(transaction_data->>'Date','yyyy-mm-dd') as TRANSACTION_DATE,type.TRANSACTION_TYPE_ID as TRANSACTION_TYPE,cust.CUSTOMER_ID as CUSTOMER_ID,transaction_data->>'Terms' as TERMS,TO_NUMBER(transaction_data->>'Amount','L9G999g999.99') as TOTAL_AMOUNT from datalake.transaction_raw_data src left join TRANSACTIONS.DIM_CUSTOMER as cust on cust.customer_ssn=src.transaction_data->>'SSN' left join TRANSACTIONS.DIM_TRANSACTION_TYPE type on type.TRANSACTION_TYPE_NAME=src.transaction_data->>'Type' where src.etl_load_time::date=current_date) src left join TRANSACTIONS.FACT_TRANSACTION trg on src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID where trg.TRANSACTION_ID is null and trg.SUB_TRANSACTION_ID is null "
            cur.execute(fact_transaction_insert_sql)
            print('FACT_TRANSACTION INSERTED SUCCESSFULLY')
            cur.close()
            conn.close()
            print('FACT_TRANSACTION LOADED SUCCESSFULLY')
        except:
            errMsg = traceback.format_exc()
            print('Not able to load  data to fact_transaction because of following error ' + errMsg)
