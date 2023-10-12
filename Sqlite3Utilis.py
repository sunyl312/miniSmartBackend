import sqlite3
from sqlite3 import OperationalError


class sqlite3db():
    @staticmethod
    def dict_factory(cursor, row):  # 要有这个，后续输出的结果才可以按照key的形式查找
        """make results as dict format instead of tuple.
        Args:
            cursor (cursor): cursor
            row (row): row

        Returns:
            dict: list of dict
        """
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self, dbname) -> None:
        self.conn = sqlite3.connect(dbname,check_same_thread=False)
        self.conn.row_factory = sqlite3db.dict_factory
        self.cur = self.conn.cursor()
        # self.innitTable()

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self):
        return self.conn.commit()

    def execute(self, sql, params=None):
        try:
            if params == None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, params)
        except Exception as e:
            self.conn.rollback()

    def close(self):
        self.conn.close()

    def innitCRRtable(self):
        create_CRR_sql = """
        create TABLE if not exists CRR( 
        CRA_ACC varchar(255) ,
        CRR_ACC varchar(255) PRIMARY KEY ,
        CRX_ACC varchar(255) ,
        SAMC_ACC varchar(255) ,
        PRJC_ACC varchar(255) ,
        SAMPLE_type int ,
        archive_path varchar(255) ,
        SRA_PRJ_ACC varchar(255) ,
        SRA_SAMPLE varchar(255) ,
        SRA_SRX varchar(255) ,
        SRA_SRR varchar(255) ,
        SRA_SRA varchar(255) ,
        SRA_SUBMISSION varchar(255)
        );"""

        self.execute(create_CRR_sql)
        print("Create CRR table success")
        self.commit()
        return True

    def innitTASKtable(self):
        create_TASK_sql = """
        create TABLE if not exists TASK( 
        CRA_ACC varchar(255) PRIMARY KEY ,
        XML_STATUS int ,
        FQ_STATUS int ,
        UPLOAD_XML int ,
        SUBMIT_STATUS int ,
        REPORT_MD5 varchar(255) ,
        TASK_PATH varchar(255) ,
        REPORT_XML_STATUS int,
        SUBMIT_TIME datetime,
        MODIFY_TIME datetime
        );
        """
        #
        self.execute(create_TASK_sql)
        print("Create TASK table success")
        self.commit()
        return True

    def innitLOCKtable(self):
        create_LOCK_sql = """
        create TABLE if not exists LOCK( 
        SAMC_ACC varchar(255) PRIMARY KEY ,
        PRJC_ACC varchar(255) ,
        IS_SAMC_LOCK int ,
        IS_PRJC_LOCK int
        );
        """
        #
        self.execute(create_LOCK_sql)
        print("Create LOCK table success")
        self.commit()
        return True

    def createINDEX(self):
        sql = """
        CREATE INDEX crr_index
        ON CRR (CRA_ACC,CRR_ACC,CRX_ACC,SAMC_ACC,PRJC_ACC,SRA_PRJ_ACC,SRA_SAMPLE,SRA_SRX,SRA_SRR,SRA_SRA,SRA_SUBMISSION); """
        self.execute(sql)

        sql = """
        CREATE INDEX task_index
        ON TASK (CRA_ACC); """
        self.execute(sql)

        sql = """
        CREATE INDEX lock_index
        ON LOCK (SAMC_ACC,PRJC_ACC); """
        self.execute(sql)
        print("Create index on CRR, TASK, LOCK successfully")
        self.commit()

    def innitTable(self):
        self.innitCRRtable()
        self.innitTASKtable()
        self.innitLOCKtable()
        self.createINDEX()

    def innsertCRRtable(self, eachcrr):
        # print(eachcrr)
        if not (any(item.startswith("PRJNA") for item in eachcrr) or any(item.startswith("SAMN") for item in eachcrr)):
            sql = """
                insert into CRR(
                        CRA_ACC ,
                        CRR_ACC,
                        CRX_ACC,
                        SAMC_ACC,
                        PRJC_ACC ,
                        SAMPLE_type ,
                        archive_path
                        )VALUES (
                        ?,?,?,?,?,?,?
                        );
                """
        elif any(item.startswith("PRJNA") for item in eachcrr) and any(item.startswith("SAMN") for item in eachcrr):
            sql='''
                insert into CRR(
                    CRA_ACC ,
                    CRR_ACC,
                    CRX_ACC,
                    SAMC_ACC,
                    PRJC_ACC ,
                    SAMPLE_type ,
                    archive_path,
                    SRA_PRJ_ACC,
                    SRA_SAMPLE
                    )VALUES (
                    ?,?,?,?,?,?,?,?,?
                    );'''
        elif any(item.startswith("PRJNA") for item in eachcrr) and not any(item.startswith("SAMN") for item in eachcrr):
            sql='''
                insert into CRR(
                    CRA_ACC ,
                    CRR_ACC,
                    CRX_ACC,
                    SAMC_ACC,
                    PRJC_ACC ,
                    SAMPLE_type ,
                    archive_path,
                    SRA_PRJ_ACC
                    )VALUES (
                    ?,?,?,?,?,?,?,?
                    );'''
        # print(sql)
        self.execute(sql=sql, params=eachcrr)
        self.commit()

    def innserTASKtable(self, CRAacc):
        sql = """
            insert into TASK(
                CRA_ACC ,
                XML_STATUS,
                FQ_STATUS,
                UPLOAD_XML,
                SUBMIT_STATUS ,
                REPORT_MD5 ,
                TASK_PATH ,
                REPORT_XML_STATUS,
                SUBMIT_TIME,
                MODIFY_TIME
                )VALUES (
                ?,?,?,?,?,?,?,?,?,?
                );
            """
        # print(sql)
        self.execute(sql=sql, params=CRAacc)
        self.commit()

    def insertLOCKtable(self, eachsamc):
        sql = """
            insert into LOCK(
            SAMC_ACC,
            PRJC_ACC,
            IS_SAMC_LOCK,
            IS_PRJC_LOCK
                )VALUES (
                ?,?,?,?
                );
            """
        self.execute(sql=sql, params=eachsamc)
        self.commit()

    def fetchfromCRR(self, acctype, acc):
        sql = f"""
            select * from CRR where {acctype}=\"{acc}\";
            """
        self.execute(sql)
        # print(sql)
        res = self.fetchall()
        self.commit()
        return res

    def fetchfromTASK(self, CRA):
        sql = f"""
            select * from TASK where CRA_ACC="{CRA}";
            """
        self.execute(sql)
        res = self.fetchall()
        self.commit()
        return res
    
    def fetchallsubmissionInTASK(self):
        sql='''
select * from TASK;'''
        res=self.conn.execute(sql).fetchall()
        return res

    def fetchfromTASKbyPageSort(self,page_number,page_size,sort_value,sort_type,filter_item,filter_value):
        print(page_number,page_size,sort_value,sort_type,filter_item,filter_value)
        start=(page_number-1)* page_size
        if sort_value!="" and sort_type!="":
            if filter_value in [0,1,2,3,4]:
                sql=f'''
            select * from TASK  where SUBMIT_STATUS={filter_value} ORDER BY {sort_value} {sort_type} limit {start},{page_size};'''
            else:
                sql=f'''
                select * from TASK ORDER BY {sort_value} {sort_type} limit {start},{page_size};'''
        elif sort_value=="" and sort_type=="":
            if filter_value in [0,1,2,3,4]:
                sql=f'''
                select * from TASK  where SUBMIT_STATUS={filter_value} limit {start},{page_size};'''
            else:
                sql=f'''
            select * from TASK  limit {start},{page_size};'''
        res=self.conn.execute(sql).fetchall()
        return res

    def fetchPRJNAfromCRR(self,acc):
        sql=f"""
        select distinct(SRA_PRJ_ACC) from CRR where PRJC_ACC="{acc}";
"""
        sraprj_acc=self.conn.execute(sql).fetchone()
        self.commit()
        if sraprj_acc==None:
            sraprj_acc={'SRA_PRJ_ACC': None}
        return sraprj_acc
    
    # def fetchAccessionInCRR(self,acctype,acc):
    #     sql=f'''
    #         select distinct(*) from CRR where {acctype}="{acc}";'''
    #     res=self.conn.execute(sql).fetchone()
    #     return res
    
    def fetchSAMNfromCRR(self,acc):
        sql=f'''
        select distinct(SRA_SAMPLE) from CRR where SAMC_ACC="{acc}";'''
        srasamn_acc=self.conn.execute(sql).fetchone()
        self.commit()
        if srasamn_acc==None:
            srasamn_acc={'SRA_SAMPLE': None}
        return srasamn_acc
        
    def fetchaccessionInTASK(self):
        sql = '''
select CRA_ACC from TASK;'''
        self.execute(sql)
        res = self.fetchall()
        # return res
        cra_list = []
        for i in res:
            # print(i)
            cra_list.append(str(i["CRA_ACC"]))
        return cra_list

    def fetchfromLOCK(self, type, acc):
        sql = f'''
            select * from LOCK where {type}="{acc}";
            '''
        self.execute(sql)
        res = self.fetchone()
        self.commit()
        return res

    def FetchAllData(self):  # 这个返回给前端所有得信息。
        sql = f'''
        SELECT
    c.CRA_ACC,
    c.CRR_ACC,
    c.CRX_ACC,
    c.SAMC_ACC,
    c.PRJC_ACC,
    c.SAMPLE_type,
    c.archive_path,
    c.SRA_PRJ_ACC,
    c.SRA_SAMPLE,
    c.SRA_SRX,
    c.SRA_SRR,
    c.SRA_SRA,
    c.SRA_SUBMISSION,
    t.XML_STATUS,
    t.FQ_STATUS,
    t.UPLOAD_XML,
    t.SUBMIT_STATUS,
    t.REPORT_MD5,
    t.TASK_PATH,
    t.REPORT_XML_STATUS,
    l.IS_SAMC_LOCK,
    l.IS_PRJC_LOCK
FROM CRR c
LEFT JOIN TASK t ON c.CRA_ACC = t.CRA_ACC
LEFT JOIN LOCK l ON c.SAMC_ACC = l.SAMC_ACC AND c.PRJC_ACC = l.PRJC_ACC;'''
        res = self.cur.execute(sql).fetchall()
        result = {}
        for eachcrr in res:
            cra_acc = eachcrr['CRA_ACC']
            crr_acc = eachcrr['CRR_ACC']
            crx_acc = eachcrr['CRX_ACC']
            samc_acc = eachcrr['SAMC_ACC']
            prjc_acc = eachcrr["PRJC_ACC"]
            sample_type = eachcrr["SAMPLE_type"]
            archive_path = eachcrr["archive_path"]
            sra_prj_acc = eachcrr["SRA_PRJ_ACC"]
            sra_sample = eachcrr["SRA_SAMPLE"]
            sra_srx = eachcrr["SRA_SRX"]
            sra_srr = eachcrr["SRA_SRR"]
            sra_sra = eachcrr["SRA_SRA"]
            sra_submission = eachcrr["SRA_SUBMISSION"]
            xml_status = eachcrr["XML_STATUS"]
            fq_status = eachcrr["FQ_STATUS"]
            upload_xml = eachcrr["UPLOAD_XML"]
            submit_status = eachcrr["SUBMIT_STATUS"]
            report_md5 = eachcrr["REPORT_MD5"]
            task_path = eachcrr["TASK_PATH"]
            report_xml_status = eachcrr["REPORT_XML_STATUS"]
            is_samc_lock = eachcrr["IS_SAMC_LOCK"]
            is_prj_lock = eachcrr["IS_PRJC_LOCK"]

            if cra_acc not in result:
                result[cra_acc] = {
                    'CRA_ACC': cra_acc,
                    "PRJC_ACC": prjc_acc,
                    "SAMPLE_type": sample_type,
                    "archive_path": archive_path,
                    "XML_STATUS": xml_status,
                    "FQ_STATUS": fq_status,
                    "UPLOAD_XML": upload_xml,
                    "SUBMIT_STATUS": submit_status,
                    "REPORT_MD5": report_md5,
                    "TASK_PATH": task_path,
                    "REPORT_XML_STATUS": report_xml_status,
                    "IS_PRJC_LOCK": is_prj_lock,
                    "SRA_PRJ_ACC": sra_prj_acc,
                    "SRA_SRA": sra_sra,
                    "SRA_SUBMISSION": sra_submission,
                    'CRR_ACC': [],
                    'CRX_ACC': [],
                    "SAMC_ACC": [],
                    "SRA_SAMPLE": [],
                    "SRA_SRX": [],
                    "SRA_SRR": [],
                    "IS_SAMC_LOCK": []
                }

            result[cra_acc]['CRR_ACC'].append(crr_acc)
            result[cra_acc]['CRX_ACC'].append(crx_acc)
            result[cra_acc]['SAMC_ACC'].append(samc_acc)
            result[cra_acc]['SRA_SAMPLE'].append(sra_sample)
            result[cra_acc]['SRA_SRX'].append(sra_srx)
            result[cra_acc]['SRA_SRR'].append(sra_srr)
            result[cra_acc]['IS_SAMC_LOCK'].append(is_samc_lock)
        return result

    def fetchPrjSamcAccandStatusByCRAacc(self, CRAacc):
        sql = f'''SELECT
    c.SAMC_ACC,
    c.PRJC_ACC,
    c.SRA_PRJ_ACC,
    c.SRA_SAMPLE,
    l.IS_SAMC_LOCK,
    l.IS_PRJC_LOCK FROM CRR c LEFT JOIN LOCK l ON c.SAMC_ACC = l.SAMC_ACC AND c.PRJC_ACC = l.PRJC_ACC WHERE c.CRA_ACC = "{CRAacc}";'''
        res = self.conn.execute(sql).fetchall()
        self.commit()
        return res

    def updateCRAincrr(self, SRX,SRR,SRA,SUBMISSION,CRRacc):
        sql = """
        update CRR set SRA_SRX="{}",SRA_SRR="{}",SRA_SRA="{}",SRA_SUBMISSION="{}" where CRR_ACC="{}";""".format(SRX,SRR,SRA,SUBMISSION,CRRacc)
        # print(sql)
        self.execute(sql)
        self.commit()

    def updatePRJinCRR(self,sraprj,gsaprj):
        sql=f'''
            update CRR set SRA_PRJ_ACC="{sraprj}" where PRJC_ACC="{gsaprj}";'''
        self.execute(sql)
        self.commit()

    
    def updateSAMPLEinCRR(self,srasamc,gsasamc):
        sql=f'''
            update CRR set SRA_SAMPLE="{srasamc}" where SAMC_ACC="{gsasamc}";'''
        self.execute(sql)
        self.commit()


    def updateTASKtable(self, statustype, status, acc):
        sql = f"""
            update TASK set {statustype}="{status}" where CRA_ACC="{acc}";
            """
        self.execute(sql)
        self.commit()

    def updateLOCKstatus(self, acctype, acc, locktype, status):
        sql = f"""
            update LOCK set {locktype}={status} where {acctype}="{acc}";
            """
        # print(sql)
        self.execute(sql)
        self.commit()


if __name__ == "__main__":
    sq = sqlite3db("GSA2SRA.db")
    # a=sq.fetchPrjSamcAccandStatusByCRAacc("CRA012787")
    # print(a)
    # res1=sq.fetchfromTASKbyPageSort(1,2,"CRA_ACC","asc")
    # res2=sq.fetchfromTASKbyPageSort(1,2,"","")
    # print(res1)
    # print(res2)
    # idList={"page_number":1,"page_size":2,"sort_value":'',"sort_type":''}
    # page_number=idList["page_number"]
    # page_size=idList["page_size"]
    # sort_value=idList["sort_value"]
    # sort_type=idList["sort_type"]
    # res2=sq.fetchfromTASKbyPageSort(page_number,page_size,sort_value,sort_type)
    # print(res2)
    # a = sq.fetchfromCRR("CRR_ACC", "CRR000061")
    # a = sq.fetchfromTASK("CRA000005")
    # print(len(a))
    # cra_acc = ["CRA000005", 0, 0, 0, 0, "", "", 0]
    # sq.innserTASKtable(cra_acc)
    # a = sq.fetchfromTASK("CRA000005")
    # sq.updateTASKstatus("SUBMIT_STATUS", 4, "CRA000015")
    # print(a[0][0])
    # sq.FetchAllData()
    # a = [
    #     {'CRA_ACC': 'CRA000019', 'CRR_ACC': 'CRR000221', 'CRX_ACC': 'CRX000265'},
    #     {'CRA_ACC': 'CRA000019', 'CRR_ACC': 'CRR000222', 'CRX_ACC': 'CRX000266'},
    #     {'CRA_ACC': 'CRA000019', 'CRR_ACC': 'CRR000223', 'CRX_ACC': 'CRX000267'},
    #     {'CRA_ACC': 'CRA000020', 'CRR_ACC': 'CRR000229', 'CRX_ACC': 'CRX000268'}]

    # result = {}

    # for item in a:
    #     cra_acc = item['CRA_ACC']
    #     crr_acc = item['CRR_ACC']
    #     crx_acc = item['CRX_ACC']

    #     if cra_acc not in result:
    #         result[cra_acc] = {
    #             'CRA_ACC': cra_acc,
    #             'CRR_ACC': [],
    #             'CRX_ACC': []
    #         }

    #     result[cra_acc]['CRR_ACC'].append(crr_acc)
    #     result[cra_acc]['CRX_ACC'].append(crx_acc)
    # print(result)

    # formatted_result = {}
    # for cra_acc, data in result.items():
    #     formatted_result[cra_acc] = data

    # print(formatted_result)
    sra_samn=sq.fetchSAMNfromCRR("SAMC000206")
    sra_prjna=sq.fetchPRJNAfromCRR("PRJCA020116")
    print(sra_samn["SRA_SAMPLE"])
    print(sra_prjna["SRA_PRJ_ACC"])
