import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s:] %(message)s')

class db():
    class db():

    @staticmethod #使用@staticmethod或@classmethod，就可以不需要实例化，直接类名.方法名()来调用。
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d 

    def __init__(self,dbname):
        self.conn = sqlite3.connect(dbname)
        self.conn.row_factory = db.dict_factory
        self.cur = self.conn.cursor()

    def execute(self,sql,params=None):
        try:
            if params is None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql,params)
        except Exception as e :
            self.conn.rollback()
            logging.error(f"Execute sql {sql} enconters error:{e}") 
    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()
    def commit(self):
        self.conn.commit()       
    def close(self):
        self.conn.close()

    def initTaxTable(self):

        dropTableSql="drop table if exists publication;"

        createTableSql = """
                        create table if not exists publication ( 
                        ID	 integer not null PRIMARY KEY AUTOINCREMENT , 
                        Journal text not null, 
                        Publisher	 text not null, 
                        ISSN	 text not null, 
                        EISSN	 text not null, 
                        Total_Articles	 text not null, 
                        Total_OA_Articles	 text not null,
                        5-Year_Citations	 text not null, 
                        H5_Index	 text not null, 
                        Monthly_Citation_Metric	 text not null,
                        );
                        """

    

        try:
            self.cur.execute(dropTableSql)
            self.conn.commit()
            self.cur.execute(createTableSql)
            self.conn.commit()
         
        except Exception as e :
            self.conn.rollback()
            logging.error(f"Execute sql {createTableSql} enconters error:{e}")
            