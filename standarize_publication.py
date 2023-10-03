import sqlite3
import os


class config():
    db_name = "journal_table.db"


class sqlite3db():

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d 

    def __init__(self,dbname) -> None:
        self.conn = sqlite3.connect(dbname)
        self.conn.row_factory = sqlite3db.dict_factory
        self.cur = self.conn.cursor()
    
    def execute(self,sql,params = None):
        try:
            if params == None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql,params)
        except Exception as e:
            self.conn.rollback()
            print(f"execution failed with sql: {sql}, reason: {str(e)}")
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
                        Nr	 integer not null PRIMARY KEY,
                        Journal	 text not null,
                        Publisher	 text not null,
                        ISSN	 text not null,
                        EISSN	 text not null,
                        Total_Articles	 text not null,
                        Total_OA_Articles	 text not null,
                        Year_Citations_5Year	 text not null,
                        H5_Index	 text not null,
                        Monthly_Citation_Metric	 text not null,
                        Journal_std	 text not null
                        );
                        """       
        self.cur.execute(dropTableSql)
        self.conn.commit()
        self.cur.execute(createTableSql)
        self.conn.commit()

    def insertOneLine(self,ll):
        sql = """
        Insert into publication (
                        Nr	,
                        Journal ,
                        Publisher ,
                        ISSN ,
                        EISSN ,
                        Total_Articles ,
                        Total_OA_Articles ,
                        Year_Citations_5Year ,
                        H5_Index ,
                        Monthly_Citation_Metric,
                        Journal_std
        ) 
        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?
        )
        """
        self.execute(sql=sql,params = ll)
    def fetchSimilarJournals(self,q):
        sql = f"""
        select * from publication where Journal_std = '{q}';
        """
        print(sql)
        self.execute(sql=sql)
        return self.fetchall()

def query(q_list):
    
    sqldb2 = sqlite3db(config.db_name)

    qres = {}
    for oneq in q_list:
        if str(oneq).strip() == "":
            continue
        oneq_clean = str(oneq).strip().upper()
        qres[oneq] = sqldb2.fetchSimilarJournals(oneq_clean)
    return qres


def buildSqlite3DB(journal_table):
    
    if not os.path.exists(journal_table):
        print(f"Journal table {journal_table} does not exists")
        exit(1)
    
    sqldb = sqlite3db(config.db_name)
    sqldb.initTaxTable()
    with open(journal_table) as fjournal_talbe:
        next(fjournal_talbe)
        for one_line in fjournal_talbe:
            line_list = one_line.strip().split("\t")
            line_len = len(line_list)
            if line_len < 10:
                line_list.extend([0 for i in range(10-line_len)])
            line_list.append(line_list[1].upper().strip())
            sqldb.insertOneLine(line_list)
        sqldb.commit()
        sqldb.close()
    print("Insert journal table successfully.")
        
if __name__ == "__main__":

    buildSqlite3DB("./journal_table.txt")
    # import json
    # print(
    #     json.dumps(
    #         query([
    #             "Applications in Energy and Combustion Scie",
    #             "Nature",
    #             "Statisztikai Szem",
    #             "Science"
    #         ]),
    #         indent=4
    #     )

    # )