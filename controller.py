# import pymysql
import psycopg2

class MysqlController:
    def __init__(self, host, port, id, pw, db_name):
        self.host = host
        self.port = port
        self.db_name = db_name
        # self.conn = pymysql.connect(host=host, port=int(port), user=id, password=pw, db=db_name, charset='utf8')
        self.conn = psycopg2.connect(host=host, port=int(port), user=id, password=pw, dbname=db_name)
        self.curs = self.conn.cursor()

    def _connection_info(self):
        print("*** CONNECTION INFORMATION ***")
        print(f"*  host : {self.host}")
        print(f"*  port : {self.port}")
        print(f"*  db_name : {self.db_name}")
        print('*'*30)
