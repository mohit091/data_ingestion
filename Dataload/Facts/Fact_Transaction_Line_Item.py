from Utils import connection
import traceback


class fact_transaction_line_item_load():
    '''Class to perform all the ingestion activities for fact_transaction_line_item'''

    def data_load(self, host, port, dbname, user, password):
        '''function to load data to fact transaction line item table'''
        try:
            host = host
            port = port
            dbname = dbname
            user = user
            password = password
            conn_details = connection.database_connection.connection(host, port, dbname, user, password)
            conn = conn_details[0]
            cur = conn_details[1]
            fact_transaction_line_item_update_sql = "update TRANSACTIONS.FACT_TRANSACTION_LINE_ITEMS trg set PRODUCT_NAME=src.PRODUCT_NAME,UNIT_NAME=src.UNIT_NAME,TOTAL_QUANTITY=src.TOTAL_QUANTITY from (with cte as (select REPLACE(transaction_data->>'LineItems'::text,'\','')::jsonb as transaction_data,SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID from datalake.transaction_raw_data where etl_load_time::date=current_date)select distinct cte.transaction_id,cte.sub_transaction_id, id as TRANSACTION_LINE_ITEM_ID,name as PRODUCT_NAME,unit as UNIT_NAME,quantity as TOTAL_QUANTITY from cte,jsonb_to_recordset(cte.transaction_data) as x(\"id\" int, \"name\" text,\"unit\" text,\"quantity\" int)) src where src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID and src.TRANSACTION_LINE_ITEM_ID=trg.TRANSACTION_LINE_ITEM_ID"
            cur.execute(fact_transaction_line_item_update_sql)
            print('FACT_TRANSACTION_LINE_ITEM UPDATED SUCCESSFULLY')
            fact_transaction_line_item_insert_sql = "insert into TRANSACTIONS.FACT_TRANSACTION_LINE_ITEMS (TRANSACTION_ID,SUB_TRANSACTION_ID,TRANSACTION_LINE_ITEM_ID,PRODUCT_NAME,UNIT_NAME,TOTAL_QUANTITY) select src.TRANSACTION_ID,src.SUB_TRANSACTION_ID,src.TRANSACTION_LINE_ITEM_ID,src.PRODUCT_NAME,src.UNIT_NAME,src.TOTAL_QUANTITY from (with cte as (select REPLACE(transaction_data->>'LineItems'::text,'\','')::jsonb as transaction_data,SPLIT_PART(transaction_data->>'TransactionId','-',1) as TRANSACTION_ID,coalesce(nullif(split_part(transaction_data->>'TransactionId','-',2),'')::int,0) as SUB_TRANSACTION_ID from datalake.transaction_raw_data where etl_load_time::date=current_date) select distinct cte.transaction_id,cte.sub_transaction_id, id as TRANSACTION_LINE_ITEM_ID,name as PRODUCT_NAME,unit as UNIT_NAME,quantity as TOTAL_QUANTITY from cte,jsonb_to_recordset(cte.transaction_data) as x(\"id\" int, \"name\" text,\"unit\" text,\"quantity\" int)) src left join TRANSACTIONS.FACT_TRANSACTION_LINE_ITEMS trg on src.TRANSACTION_ID=trg.TRANSACTION_ID and src.SUB_TRANSACTION_ID=trg.SUB_TRANSACTION_ID and src.TRANSACTION_LINE_ITEM_ID=trg.TRANSACTION_LINE_ITEM_ID where trg.TRANSACTION_ID is null and trg.SUB_TRANSACTION_ID is null and trg.TRANSACTION_LINE_ITEM_ID is null"
            cur.execute(fact_transaction_line_item_insert_sql)
            print('FACT_TRANSACTION_LINE_ITEM INSERTED SUCCESSFULLY')
            cur.close()
            conn.close()
            print('DIM_CUSTOMER LOADED SUCCESSFULLY')
        except:
            errMsg = traceback.format_exc()
            print('Not able to load  data to fact_transaction_line_item because of following error ' + errMsg)
