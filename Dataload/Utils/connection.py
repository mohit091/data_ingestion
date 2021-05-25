import psycopg2
import traceback

class database_connection():
        @staticmethod
        def connection(host,port,dbname,user,password):
            try:
                host=host
                port=port
                dbname=dbname
                user=user
                password=password
                conn = psycopg2.connect(host=host ,port=port ,dbname=dbname ,user=user ,password=password)
                conn.autocommit = True
                cur = conn.cursor()
                print ('Connection to database Successfull')
                return conn,cur

            except:
                errMsg = traceback.format_exc()
                print('Not able to connect to data beacuse of following error ' + errMsg )

