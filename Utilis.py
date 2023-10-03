from UserPackage import UserPackage
import Utilis
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime
import uuid
import paramiko
public_root_dict = UserPackage("public_root_dict")
archive_root_dict = UserPackage("archive_root_dict")


CheckAcc_dict = {
    "CRAacc": "select * from cra where accession=\"#data#\";",
    "subCRA": "select * from cra where cra_id=\"#data#\";",
    "SAMCacc": "select * from sample where accession=\"#data#\";",
    "CRXacc": "select * from experiment where accession=\"#data#\";",
    "CRRacc": "select * from run where accession=\"#data#\";"
}

CheckCount_dict = {
    "unfinished": "select * from cra where status in (11,12,13,14,15,16);",
    "submitting": "select * from cra where status in (2);",
    "archive": "select * from cra where status in (3);",
    "released": "select * from cra where status in (3) and release_state=2;",
    "processError": "select * from cra where status in (4);"
}

checkRelated_id = {
    "CRAacc": "SELECT ca.*, u.email, u.cas_user_id FROM `user` AS u JOIN ( SELECT cr.*, prj.accession AS prj_accession, prj.user_id FROM project AS prj JOIN ( SELECT sr.*, c.STATUS, c.release_state, c.public_root, c.archive_root, c.accession AS cra_accession FROM cra AS c JOIN ( SELECT er.*, s.accession AS sample_accession FROM sample AS s JOIN ( SELECT r.*, e.accession AS experiment_acc FROM experiment AS e JOIN ( SELECT run.run_id, run.exp_id, run.cra_id, run.sample_id, run.prj_id, run.accession AS run_accession FROM run WHERE cra_id IN ( SELECT cra_id FROM cra WHERE accession IN ( #data# ) )) AS r ON e.exp_id = r.exp_id ) AS er ON s.sample_id = er.sample_id ) AS sr ON c.cra_id = sr.cra_id ) AS cr ON prj.prj_id = cr.prj_id ) AS ca ON u.user_id = ca.user_id;",
    "subCRA": "SELECT cr.*, u.email, u.cas_user_id FROM `user` AS u JOIN ( SELECT cr.*, prj.accession AS prj_accession FROM project AS prj JOIN ( SELECT sr.*, c.STATUS, c.release_state, c.public_root, c.archive_root, c.user_id, c.accession AS cra_accession FROM cra AS c JOIN ( SELECT er.*, s.accession AS sample_accession FROM sample AS s JOIN ( SELECT r.*, e.accession AS experiment_acc FROM experiment AS e JOIN ( SELECT run.run_id, run.exp_id, run.cra_id, run.sample_id, run.prj_id, run.accession AS run_accession FROM run WHERE cra_id IN ( SELECT cra_id FROM cra WHERE cra_id IN ( #data# ))) AS r ON e.exp_id = r.exp_id ) AS er ON s.sample_id = er.sample_id ) AS sr ON c.cra_id = sr.cra_id ) AS cr ON prj.prj_id = cr.prj_id ) AS cr ON u.user_id = cr.user_id;",
    "SAMCacc": "SELECT sa.*, exp.accession AS experiment_acc FROM experiment AS exp JOIN ( SELECT prja.*, accession AS sample_accession FROM sample AS sample JOIN ( SELECT ua.*, prj.accession AS prj_accession FROM project AS prj JOIN ( SELECT cr.*, u.email, u.cas_user_id FROM `user` AS u JOIN ( SELECT r.*, c.accession AS cra_accession, c.release_state, c.archive_root, c.public_root, c.STATUS, c.user_id FROM cra AS c JOIN ( SELECT run.run_id, run.exp_id, run.cra_id, run.sample_id, run.prj_id, run.accession AS run_accession FROM run WHERE sample_id IN ( SELECT sample_id FROM sample WHERE accession IN ( #data# ) )) AS r ON c.cra_id = r.cra_id ) AS cr ON u.user_id = cr.user_id ) AS ua ON ua.prj_id = prj.prj_id ) AS prja ON prja.sample_id = sample.sample_id ) AS sa ON sa.exp_id = exp.exp_id;",
    "CRXacc": "SELECT sa.*, exp.accession AS experiment_acc FROM experiment AS exp JOIN ( SELECT prja.*, accession AS sample_accession FROM sample AS sample JOIN ( SELECT ua.*, prj.accession AS prj_accession FROM project AS prj JOIN ( SELECT cr.*, u.email, u.cas_user_id FROM `user` AS u JOIN ( SELECT r.*, c.accession AS cra_accession, c.archive_root, c.public_root, c.release_state, c.STATUS, c.user_id FROM cra AS c JOIN ( SELECT run.run_id, run.cra_id, run.prj_id, run.sample_id, run.accession AS run_accession, run.exp_id FROM run WHERE exp_id IN ( SELECT exp_id FROM experiment WHERE accession IN ( #data# ))) AS r ON c.cra_id = r.cra_id ) AS cr ON u.user_id = cr.user_id ) AS ua ON ua.prj_id = prj.prj_id ) AS prja ON prja.sample_id = sample.sample_id ) AS sa ON sa.exp_id = exp.exp_id;",
    "CRRacc": "SELECT sa.*, exp.accession AS experiment_acc FROM experiment AS exp JOIN ( SELECT prja.*, accession AS sample_accession FROM sample AS sample JOIN ( SELECT ua.*, prj.accession AS prj_accession FROM project AS prj JOIN ( SELECT cr.*, u.email, u.cas_user_id FROM `user` AS u JOIN ( SELECT r.*, c.accession AS cra_accession, c.archive_root, c.release_state, c.public_root, c.STATUS, c.user_id FROM cra AS c JOIN ( SELECT run.run_id, run.cra_id, run.prj_id, run.sample_id, run.exp_id, run.accession AS run_accession FROM run WHERE accession IN ( #data#) ) AS r ON c.cra_id = r.cra_id ) AS cr ON u.user_id = cr.user_id ) AS ua ON ua.prj_id = prj.prj_id ) AS prja ON prja.sample_id = sample.sample_id ) AS sa ON sa.exp_id = exp.exp_id;",
    "PRJacc": "SELECT ua.*, prj.accession AS prj_accession FROM project AS prj JOIN ( SELECT ca.*, USER.cas_user_id, USER.email FROM `user` AS USER JOIN ( SELECT c.accession AS cra_accession, c.release_state, c.archive_root, c.public_root, c.STATUS, c.user_id, c.prj_id, c.cra_id FROM cra AS c WHERE prj_id IN ( SELECT prj_id FROM project WHERE accession IN ( (#data#) ))) AS ca ON ca.user_id = USER.user_id ) AS ua ON ua.prj_id = prj.prj_id;",
    "email": "SELECT ca.*, `user`.cas_user_id, `user`.email FROM `user` AS user JOIN ( SELECT prj.*, c.accession AS cra_accession, c.cra_id, c.release_state, c.archive_root, c.public_root, c.STATUS FROM cra AS c JOIN ( SELECT prj_id, accession AS prj_accession, user_id FROM project WHERE user_id IN ( SELECT user_id FROM `user` WHERE email in (#data#) ) ) AS prj ON prj.prj_id = c.prj_id ) AS ca ON ca.user_id = `user`.user_id;"
}


class GSAUtilis2():  # 定义链接数据库
    def __init__(self):
        userinfo = UserPackage("gsadb")
        user, passwd, host, port, name = userinfo['username'], userinfo[
            "password"], userinfo["host"], userinfo["port"], userinfo["dbname"],
        engine = create_engine(
            # 所服务器
            "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(
                user, passwd, host, port, name),
            echo=True,  # echo ：为 True 时候会把sql语句打印出来
            max_overflow=999,  # 连接池的大小，为0表示无限制
            pool_pre_ping=True  # 这是1.2新增的参数，如果值为True，那么每次从连接池中拿连接的时候，都会向数据库发送一个类似 select 1 的测试查询语句来判断服务器是否正常运行。当该连接出现 disconnect 的情况时，该连接连同pool中的其它连接都会被回收。
        )
        # 通过sessionmake方法创建一个Session工厂，然后在调用工厂的方法来实例化一个Session对象
        self.Session = sessionmaker(bind=engine)

    def execute(self, sql):
        try:
            self.session = self.Session()
            return self.session.execute(text(sql))
        except Exception as e:
            return str(e)

    def CheckRunStatus(self, craid):
        res1 = []
        sql = f'''
        select distinct(status) from run where cra_id={craid}'''
        res = self.execute(sql).fetchall()
        for i in res:
            res1.append(str(i["status"]))
        return res1

    def CheckBiosampleFailStatus(self, id):
        status = []
        sql = f'''
            select distinct(status) from sample where submissionId="{id}";'''
        res = self.execute(sql).fetchall()
        for i in res:
            status.append(str(i["status"]))
        return status

    def CheckMetaFailStatus(self, id):
        status = []
        sql = f'''
            select distinct(status) from experiment where cra_id={id};'''
        res = self.execute(sql).fetchall()
        for i in res:
            status.append(str(i["status"]))

        return status

    def CheckAcc(self, type, id):
        id1 = id.strip().replace("subCRA", "")
        sql = CheckAcc_dict[type].replace("#data#", id1)
        res = self.execute(sql).fetchone()
        if res != None:
            return {"status": True, "data": res}
        else:
            return {"status": False, "data": "None"}

    def CheckStatusCount(self):
        res = {}
        for k in CheckCount_dict.keys():
            sql = CheckCount_dict[k]
            count = self.execute(sql).fetchall()
            res[k] = len(count)
        return res

    def CheckRelatedID(self, type, id):
        id1 = f'"{id.strip().replace("subCRA", "")}"'
        sql = checkRelated_id[type].replace("#data#", id1)
        res = self.execute(sql).fetchall()
        if len(res) != 0:
            return {"status": True, "data": res}
        else:
            return {"status": False, "data": "None"}

    def ScanData2SRA(self):
        sql = """
SELECT
	cr.*,
	prj.accession AS PRJCAacc 
FROM
	project AS prj
	JOIN (
	SELECT
		sr.*,
		c.release_state,
		c.public_root,
		c.release_time,
		c.accession AS CRAacc 
	FROM
		cra AS c
		JOIN (
		SELECT
			er.*,
			s.sample_type_id,
			s.accession AS SAMCacc 
		FROM
			sample AS s
			JOIN (
			SELECT
				r.*,
				e.accession AS CRXacc 
			FROM
				experiment AS e
				JOIN (
				SELECT
					run.accession AS CRRacc,
					run.exp_id,
					run.cra_id,
					run.sample_id,
					run.prj_id 
				FROM
					run 
				WHERE
					cra_id IN ( SELECT cra_id FROM cra WHERE STATUS = 3 AND release_state = 2 AND sample_type_id != 5 ) 
				AND run_data_type_id IN ( 1, 2 ) and accession is NOT NULL) AS r ON e.exp_id = r.exp_id 
			) AS er ON s.sample_id = er.sample_id 
		) AS sr ON c.cra_id = sr.cra_id 
	) AS cr ON prj.prj_id = cr.prj_id;"""

        res = self.execute(sql).fetchall()
        return res

#     def CRAaccRelatedID(self, CRAacc):
#         CRAaccs = ','.join(['"{}"'.format(item) for item in CRAacc])
#         sql = '''
#             SELECT
# 	ca.*,
# 	u.email,
# 	u.cas_user_id
# FROM
# 	`user` AS u
# 	JOIN (
# 	SELECT
# 		cr.*,
# 		prj.accession AS prj_accession,
# 		prj.user_id
# 	FROM
# 		project AS prj
# 		JOIN (
# 		SELECT
# 			sr.*,
# 			c.STATUS,
# 			c.release_state,
# 			c.public_root,
# 			c.archive_root,
# 			c.accession AS cra_accession
# 		FROM
# 			cra AS c
# 			JOIN (
# 			SELECT
# 				er.*,
# 				s.accession AS sample_accession
# 			FROM
# 				sample AS s
# 				JOIN (
# 				SELECT
# 					r.*,
# 					e.accession AS experiment_acc
# 				FROM
# 					experiment AS e
# 					JOIN (
# 					SELECT
# 						run.run_id,
# 						run.exp_id,
# 						run.cra_id,
# 						run.sample_id,
# 						run.prj_id,
# 						run.accession AS run_accession
# 					FROM
# 						run
# 					WHERE
# 					cra_id IN ( SELECT cra_id FROM cra WHERE accession IN ( {} ) )) AS r ON e.exp_id = r.exp_id
# 				) AS er ON s.sample_id = er.sample_id
# 			) AS sr ON c.cra_id = sr.cra_id
# 		) AS cr ON prj.prj_id = cr.prj_id
# 	) AS ca ON u.user_id = ca.user_id;
#         '''.format(CRAaccs)
#         print(sql)
#         res = self.execute(sql).fetchall()
#         return res

#     def CRAidRelatedID(self, CRAid):
#         CRAids = ','.join(['"{}"'.format(item) for item in CRAid])

#         sql = '''
#         SELECT
# 	cr.*,
# 	u.email,
# 	u.cas_user_id
# FROM
# 	`user` AS u
# 	JOIN (
# 	SELECT
# 		cr.*,
# 		prj.accession AS prj_accession
# 	FROM
# 		project AS prj
# 		JOIN (
# 		SELECT
# 			sr.*,
# 			c.STATUS,
# 			c.release_state,
# 			c.public_root,
# 			c.archive_root,
# 			c.user_id,
# 			c.accession AS cra_accession
# 		FROM
# 			cra AS c
# 			JOIN (
# 			SELECT
# 				er.*,
# 				s.accession AS sample_accession
# 			FROM
# 				sample AS s
# 				JOIN (
# 				SELECT
# 					r.*,
# 					e.accession AS experiment_acc
# 				FROM
# 					experiment AS e
# 					JOIN (
# 					SELECT
# 						run.run_id,
# 						run.exp_id,
# 						run.cra_id,
# 						run.sample_id,
# 						run.prj_id,
# 						run.accession AS run_accession
# 					FROM
# 						run
# 					WHERE
# 						cra_id IN (
# 						SELECT
# 							cra_id
# 						FROM
# 							cra
# 						WHERE
# 						cra_id IN ( {} ))) AS r ON e.exp_id = r.exp_id
# 				) AS er ON s.sample_id = er.sample_id
# 			) AS sr ON c.cra_id = sr.cra_id
# 		) AS cr ON prj.prj_id = cr.prj_id
# 	) AS cr ON u.user_id = cr.user_id;
#         '''.format(CRAids)
#         res = self.execute(sql).fetchall()
#         return res

#     def SAMCaccRelatedID(self, SAMCacc):
#         SAMCaccs = ','.join(['"{}"'.format(item) for item in SAMCacc])
#         sql = '''
#             SELECT
# 	sa.*,
# 	exp.accession AS experiment_acc
# FROM
# 	experiment AS exp
# 	JOIN (
# 	SELECT
# 		prja.*,
# 		accession AS sample_accession
# 	FROM
# 		sample AS sample
# 		JOIN (
# 		SELECT
# 			ua.*,
# 			prj.accession AS prj_accession
# 		FROM
# 			project AS prj
# 			JOIN (
# 			SELECT
# 				cr.*,
# 				u.email,
# 				u.cas_user_id
# 			FROM
# 				`user` AS u
# 				JOIN (
# 				SELECT
# 					r.*,
# 					c.accession AS cra_accession,
# 					c.release_state,
# 					c.archive_root,
# 					c.public_root,
# 					c.STATUS,
# 					c.user_id
# 				FROM
# 					cra AS c
# 					JOIN (
# 					SELECT
# 						run.run_id,
# 						run.exp_id,
# 						run.cra_id,
# 						run.sample_id,
# 						run.prj_id,
# 						run.accession AS run_accession
# 					FROM
# 						run
# 					WHERE
# 					sample_id IN ( SELECT sample_id FROM sample WHERE accession IN ( {} ) )) AS r ON c.cra_id = r.cra_id
# 				) AS cr ON u.user_id = cr.user_id
# 			) AS ua ON ua.prj_id = prj.prj_id
# 		) AS prja ON prja.sample_id = sample.sample_id
# 	) AS sa ON sa.exp_id = exp.exp_id;
#     '''.format(SAMCaccs)
#         res = self.execute(sql).fetchall()
#         return res

#     def CRXaccRelatedID(self, CRXacc):
#         crxaccs = ','.join(['"{}"'.format(item) for item in CRXacc])
#         sql = '''
# SELECT
# 	sa.*,
# 	exp.accession AS experiment_acc
# FROM
# 	experiment AS exp
# 	JOIN (
# 	SELECT
# 		prja.*,
# 		accession AS sample_accession
# 	FROM
# 		sample AS sample
# 		JOIN (
# 		SELECT
# 			ua.*,
# 			prj.accession AS prj_accession
# 		FROM
# 			project AS prj
# 			JOIN (
# 			SELECT
# 				cr.*,
# 				u.email,
# 				u.cas_user_id
# 			FROM
# 				`user` AS u
# 				JOIN (
# 				SELECT
# 					r.*,
# 					c.accession AS cra_accession,
# 					c.archive_root,
# 					c.public_root,
# 					c.release_state,
# 					c.STATUS,
# 					c.user_id
# 				FROM
# 					cra AS c
# 					JOIN (
# 					SELECT
# 						run.run_id,
# 						run.cra_id,
# 						run.prj_id,
# 						run.sample_id,
# 						run.accession AS run_accession,
# 						run.exp_id
# 					FROM
# 						run
# 					WHERE
# 						exp_id IN (
# 						SELECT
# 							exp_id
# 						FROM
# 							experiment
# 						WHERE
# 						accession IN ( {} ))) AS r ON c.cra_id = r.cra_id
# 				) AS cr ON u.user_id = cr.user_id
# 			) AS ua ON ua.prj_id = prj.prj_id
# 		) AS prja ON prja.sample_id = sample.sample_id
# 	) AS sa ON sa.exp_id = exp.exp_id;
# '''.format(crxaccs)
#         res = self.execute(sql).fetchall()
#         return res

#     def CRRaccRelatedID(self, CRRacc):
#         crraccs = ','.join(['"{}"'.format(item) for item in CRRacc])
#         sql = '''
# SELECT
# 	sa.*,
# 	exp.accession AS experiment_acc
# FROM
# 	experiment AS exp
# 	JOIN (
# 	SELECT
# 		prja.*,
# 		accession AS sample_accession
# 	FROM
# 		sample AS sample
# 		JOIN (
# 		SELECT
# 			ua.*,
# 			prj.accession AS prj_accession
# 		FROM
# 			project AS prj
# 			JOIN (
# 			SELECT
# 				cr.*,
# 				u.email,
# 				u.cas_user_id
# 			FROM
# 				`user` AS u
# 				JOIN (
# 				SELECT
# 					r.*,
# 					c.accession AS cra_accession,
# 					c.archive_root,
# 					c.release_state,
# 					c.public_root,
# 					c.STATUS,
# 					c.user_id
# 				FROM
# 					cra AS c
# 					JOIN (
# 					SELECT
# 						run.run_id,
# 						run.cra_id,
# 						run.prj_id,
# 						run.sample_id,
# 						run.exp_id,
# 						run.accession AS run_accession
# 					FROM
# 						run
# 					WHERE
# 						accession IN (
# 						{})
# 					) AS r ON c.cra_id = r.cra_id
# 				) AS cr ON u.user_id = cr.user_id
# 			) AS ua ON ua.prj_id = prj.prj_id
# 		) AS prja ON prja.sample_id = sample.sample_id
# 	) AS sa ON sa.exp_id = exp.exp_id;'''.format(crraccs)
#         res = self.execute(sql).fetchall()
#         return res

#     def emailRelatedID(self, email):
#         emails = ','.join(['"{}"'.format(item) for item in email])
#         sql = '''
# SELECT
# 	ca.*,
# 	`user`.cas_user_id,
# 	`user`.email
# FROM
# 	`user`
# 	AS user JOIN (
# 	SELECT
# 		prj.*,
# 		c.accession AS cra_accession,
# 		c.cra_id,
# 		c.release_state,
# 		c.archive_root,
# 		c.public_root,
# 		c.STATUS
# 	FROM
# 		cra AS c
# 	JOIN ( SELECT prj_id, accession AS prj_accession, user_id FROM project WHERE user_id IN ( SELECT user_id FROM `user` WHERE email in  ({}) ) ) AS prj ON prj.prj_id = c.prj_id
# 	) AS ca ON ca.user_id = `user`.user_id;'''.format(emails)
#         res = self.execute(sql).fetchall()
#         return res

#     def PRJaccRelatedID(self, prjacc):
#         prjaccs = ','.join(['"{}"'.format(item) for item in prjacc])
#         sql = '''
# SELECT
# 	ua.*,
# 	prj.accession AS prj_accession
# FROM
# 	project AS prj
# 	JOIN (
# 	SELECT
# 		ca.*,
# 		USER.cas_user_id,
# 		USER.email
# 	FROM
# 		`user`
# 		AS USER JOIN (
# 		SELECT
# 			c.accession AS cra_accession,
# 			c.release_state,
# 			c.archive_root,
# 			c.public_root,
# 			c.STATUS,
# 			c.user_id,
# 			c.prj_id,
# 			c.cra_id
# 		FROM
# 			cra AS c
# 		WHERE
# 			prj_id IN (
# 			SELECT
# 				prj_id
# 			FROM
# 				project
# 			WHERE
# 			accession IN ( ({}) ))) AS ca ON ca.user_id = USER.user_id
# 	) AS ua ON ua.prj_id = prj.prj_id;'''.format(prjaccs)
#         res = self.execute(sql).fetchall()
#         return res


class FTPUtilis():
    def __init__(self):
        """_summary_
        链接21服务器
        """
        self.ssh = paramiko.SSHClient()

        self.ssh.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())  # 自动处理第一次链接的yes或no问题
        userinfo = UserPackage("ftp21")["login"]

        user, passwd, host, port = userinfo['username'], userinfo["password"], userinfo["host"], userinfo["port"]
        self.ssh.connect(
            port=port,
            hostname=host,
            username=user,
            password=passwd
        )
        print("登录21数据库成功")

    def close(self):
        self.ssh.close()

    def stat(self, file):
        return self.ssh.open_sftp().stat(file)

    # def scpclient(self):
    #     return self.SCPClient(self.ssh.get_transport(), socket_timeout=15.0)

    def exec(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return (stdout.read().decode())
        # ssh_client.open_sftp().stat(file)
        # return self.ssh.exec_command(shell)

#     # 通过这个函数判断cra或者craacc下是否还关联了其他编号，判断prj_id是否应该撤回或者删除
#     def prjidRelatedCRA(self, prjid, cra_id):
#         sql = f'''
#         select cra_id,accession from cra where prj_id={prjid} and status!=5 and cra_id not in ({cra_id});
#         '''
#         res = self.execute(sql).fetchall()
#         if len(res) == 0:
#             return True
#         else:
#             return False

#     def sampleidRelatedCRA(self, sampleid, cra_id):
#         sampleids = ','.join(['"{}"'.format(item) for item in sampleid])
#         sql = f'''
#             select distinct(cra_id) from experiment where sample_id in ({sampleids}) and cra_id not in ({cra_id}) and status!=5;'''
#         res = self.execute(sql).fetchall()
#         if len(res) == 0:
#             return True
#         else:
#             return False

#     def sampleidRelatedSampleSubmission(self, sampleid):
#         sampleids = ','.join(['"{}"'.format(item) for item in sampleid])
#         sql = f'''
# SELECT DISTINCT(sample_id) FROM sample WHERE submissionId IN
# ( SELECT DISTINCT (submissionId) FROM sample WHERE sample_id IN ({sampleids})) and sample_id not in ({sampleids});'''

#     def sampleidRelatedCRX(self, sampleid):
#         sampleids = ','.join(['"{}"'.format(item) for item in sampleid])
#         sql = f"""
# select exp_id,accession from experiment where
#             """


def UUIDgeneration():
    x = uuid.uuid1()
    return x


def path_generate(res):
    related_info = []
    if res["status"] == True:
        for eachline in res["data"]:
            each_info = {}
            each_info["prj_id"] = eachline["prj_id"]
            each_info["prj_accession"] = eachline["prj_accession"]
            each_info["cra_id"] = eachline["cra_id"]
            each_info["cra_accession"] = eachline["cra_accession"]
            each_info["sample_id"] = eachline["sample_id"]
            each_info["sample_accession"] = eachline["sample_accession"]
            each_info["exp_id"] = eachline["exp_id"]
            each_info["experiment_accession"] = eachline["experiment_acc"]
            each_info["run_id"] = eachline["run_id"]
            each_info["run_accession"] = eachline["run_accession"]
            each_info["status"] = str(eachline["STATUS"]) + \
                "_" + str(eachline["release_state"])
            each_info["user_id"] = eachline["user_id"]
            each_info["email"] = eachline["email"]
            print(eachline["public_root"])

            p = eachline["cas_user_id"]
            parts = [p[:1], p[1], p]
            each_info["FTPpath"] = f"/gpfs/submit/{'/'.join(parts)}/GSA"
            if eachline["STATUS"] == 2:
                each_info["CRRpath"] = "/gsainsdc2/gsafileprocess/temp/" + \
                    eachline["cra_accession"] + "/" + eachline["run_accession"]
            elif eachline["STATUS"] == 3 and eachline["release_state"] == 1:
                each_info["CRRpath"] = archive_root_dict[str(eachline["archive_root"])
                                                         ] + eachline["cra_accession"] + "/" + eachline["run_accession"]
            elif eachline["STATUS"] == 3 and eachline["release_state"] == 2:
                each_info["CRRpath"] = public_root_dict[str(eachline["public_root"])
                                                        ] + eachline["cra_accession"] + "/" + eachline["run_accession"]
            related_info.append(each_info)

    else:
        related_info = [{"nodata"}]

    return related_info


def path_generate_for_email_prjacc(res):
    related_info = []
    if res["status"] == True:
        for eachline in res["data"]:
            each_info = {}
            each_info["prj_id"] = eachline["prj_id"]
            each_info["prj_accession"] = eachline["prj_accession"]
            each_info["cra_id"] = eachline["cra_id"]
            each_info["cra_accession"] = eachline["cra_accession"]
            each_info["status"] = str(eachline["STATUS"]) + \
                "_" + str(eachline["release_state"])
            each_info["user_id"] = eachline["user_id"]
            each_info["email"] = eachline["email"]
            print(eachline["public_root"])

            p = eachline["cas_user_id"]
            parts = [p[:1], p[1], p]
            each_info["FTPpath"] = f"/gpfs/submit/{'/'.join(parts)}/GSA"
            if eachline["STATUS"] == 2:
                each_info["CRRpath"] = "/gsainsdc2/gsafileprocess/temp/" + \
                    eachline["cra_accession"]
            elif eachline["STATUS"] == 3 and eachline["release_state"] == 1:
                each_info["CRRpath"] = archive_root_dict[str(eachline["archive_root"])
                                                         ] + eachline["cra_accession"]
            elif eachline["STATUS"] == 3 and eachline["release_state"] == 2:
                each_info["CRRpath"] = public_root_dict[str(eachline["public_root"])
                                                        ] + eachline["cra_accession"]
            related_info.append(each_info)

    else:
        related_info = [{"nodata"}]
    return related_info


if __name__ == "__main__":
    # pass
    from GSAManager import GSAutilis
    gsautilis = GSAutilis()
    gsautilis2 = GSAUtilis2()

    # print(UUIDgeneration())
    # print(checkRelated_id["subCRA"])

    # CRA "CRA002033", "CRA001985"
    # cra_id 2121,2162
    # sample accession "SAMC111158", "SAMC111157"
    # experiment accession "CRX065621", "CRX065620"
    # CRR accession "CRR078042","CRR078041"

    # a = gsautilis2.CheckRelatedID("CRAacc", "CRA002033")
    # a = gsautilis2.CheckRelatedID("email", "jiahangxing@163.com")
    # path_generate_for_email_prjacc(a)
    a = gsautilis2.ScanData2SRA()
    print(a)

    # join_result = "/workspace/project/GSA/GSA_XML/CRA001817_submission.xml"
    # with open(join_result, "r") as file:
    #     file_content = file.read()
    #     print(file_content)

    # idList = [
    #     {"type": "subSAM", "value": "subSAM111729"},
    #     {"type": "subSAM", "value": "subSAM111538"}
    # ]

    # def ResetbiosampleStatus(idList):
    #     res = []
    #     for eachid in idList:
    #         if eachid["type"] == "subSAM":
    #             id = eachid["value"]
    #             status = gsautilis2.CheckBiosampleFailStatus(id)
    #             if "3" in status:
    #                 sql = [f"update sample set status=2 where submissionId=\"{id}\";",
    #                        f"update sample_submission set status=2 where sample_submissionId=\"{id}\";"]
    #                 eachid["status"] = True
    #                 eachid["mysql"] = sql
    #             else:
    #                 eachid["status"] = False
    #             res.append(eachid)
    #     print(res)

    # ResetbiosampleStatus(idList)

    # idList = [
    #     {"type": "subCRA", "value": "subCRA018812"},
    #     {"type": "subCRA", "value": "subCRA018506"}
    # ]

    # def resetmetacheckstatus(idList):
    #     res = []
    #     for eachid in idList:
    #         cra_id = eachid["value"].strip().replace("subCRA", "")
    #         status = gsautilis2.CheckMetaFailStatus(cra_id)
    #         print(status)
    #         if "4" in status:
    #             sql = [f"update experiment set status=2 where cra_id={cra_id};",
    #                    f"update run_data_file set status=2 where cra_id={cra_id};", f"update run set status=2 where cra_id={cra_id};"]
    #             eachid["status"] = True
    #             eachid["mysql"] = sql
    #         else:
    #             eachid["status"] = False
    #         res.append(eachid)
    #     print(res)

    # resetmetacheckstatus(idList)

    # idList1 = [{
    #     'type': 'subCRA',
    #     'value': 'subCRA018549',
    #     "releasetime": "2023-12-31 00:00:00"
    # }, {
    #     'type': 'CRAacc',
    #     'value': 'CRA012521',
    #     "releasetime": "2023-12-31 00:00:00"
    # }]
    # # idList1 = [{
    # #     'type': 'subCRA',
    # #     'value': 'subCRA018549',
    # #     "releasetime": "2023-12-31 00:00:00"
    # # }]
    # # print(idList)

    # def releasecallback(idList):
    #     # cdate: str, back_id: str, cratype: str

    #     ress = []

    #     for eachid in idList:
    #         eachid["mysql"] = []
    #         eachid["ftp"] = []

    #         try:
    #             if eachid["type"] == "CRAacc":
    #                 back_id = eachid["value"].replace("cra", "CRA")
    #                 res = gsautilis.call_back_by_acc(back_id)
    #                 cra_id = res[0]
    #                 prj_id = res[1]
    #                 public_path = res[6]
    #                 archieve_path = res[5]
    #                 sample_submissionID = res[4][0][0]
    #                 new_rel_time = eachid["releasetime"]
    #                 eachid['mysql'].append('update cra set release_state=1,release_time="{}" where cra_id={};'.format(
    #                     new_rel_time, cra_id))
    #                 eachid['mysql'].append(
    #                     'update experiment set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
    #                 eachid['mysql'].append(
    #                     'update run set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
    #                 eachid['mysql'].append(
    #                     'update run_data_file set release_state=1 where cra_id={};'.format(cra_id))
    #                 if len(res[2]) == 0:
    #                     eachid['mysql'].append(
    #                         'update project set release_state=1,release_time=\"{}\" where prj_id={};'.format(new_rel_time, prj_id))
    #                 else:
    #                     pass
    #                 if len(res[3]) == 0:
    #                     eachid['mysql'].append('update sample set release_state=1,release_time=\"{}\" where submissionId=\"{}\";'.format(
    #                         new_rel_time, sample_submissionID))
    #                     eachid['mysql'].append('update sample_submission set release_state=1,release_time=\"{}\" where sample_submissionId=\"{}\";'.format(
    #                         new_rel_time, sample_submissionID))
    #                 else:
    #                     pass
    #                 eachid['ftp'].append(
    #                     "cp -r {}/{} {}/".format(public_path, back_id, archieve_path))
    #                 eachid['ftp'].append(
    #                     'rm -r {}/{}'.format(public_path, back_id))
    #                 ress.append(eachid)
    #                 print(eachid)

    #             elif eachid["type"] == "subCRA":
    #                 back_id = eachid["value"].replace("subCRA0", "")
    #                 res = gsautilis.call_back_by_cra_id(back_id)
    #                 cra_acc = res[0]
    #                 public_path = res[6]
    #                 archieve_path = res[5]
    #                 prj_id = res[1]
    #                 cra_id = back_id
    #                 sample_submissionID = res[4][0][0]
    #                 new_rel_time = eachid["releasetime"]
    #                 eachid['mysql'].append(
    #                     'update cra set release_state=1,release_time="{}" where cra_id={};'.format(new_rel_time, cra_id))
    #                 eachid['mysql'].append(
    #                     'update experiment set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
    #                 eachid['mysql'].append(
    #                     'update run set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
    #                 eachid['mysql'].append(
    #                     'update run_data_file set release_state=1 where cra_id={};'.format(cra_id))
    #                 if len(res[2]) == 0:
    #                     eachid['mysql'].append(
    #                         'update project set release_state=1,release_time=\"{}\" where prj_id={};'.format(new_rel_time, prj_id))
    #                 else:
    #                     pass
    #                 if len(res[3]) == 0:
    #                     eachid['mysql'].append('update sample set release_state=1,release_time=\"{}\" where submissionId=\"{}\";'.format(
    #                         new_rel_time, sample_submissionID))
    #                     eachid['mysql'].append('update sample_submission set release_state=1,release_time=\"{}\" where sample_submissionId=\"{}\";'.format(
    #                         new_rel_time, sample_submissionID))
    #                 else:
    #                     pass

    #                 eachid['ftp'].append(
    #                     'cp -r {}/{} {}/'.format(public_path, cra_acc, archieve_path))
    #                 eachid['ftp'].append(
    #                     'rm -rf {}/{}'.format(public_path, back_id))
    #                 ress.append(eachid)

    #         except Exception as e:
    #             break

    #     return ress
    # releasecallback(idList1)
    # print(res)

    # def problemfeedback(idList):
    #     email = idList["email"]
    #     res = {"email": email,
    #            "idlist": []}
    #     for eachid in idList["idlist"]:
    #         subCRA = eachid["value"].strip("").replace("subCRA", '')
    #         temp = autoGSAReport(subCRA, email)
    #         if temp["status"] == "ok":
    #             eachid["status"] = True
    #         else:
    #             eachid["status"] = False
    #         res["idlist"].append(eachid)

    #     return res

    # idList = {
    #     "email": "sunyanling@big.ac.cn",
    #     "idlist": [{"type": "subCRA",
    #                "value": "subCRA019765"}, {
    #         "type": "subCRA",
    #                "value": "subCRA019560"
    #                }]
    # }
    # problemfeedback(idList)

    # gsautilis2 = GSAUtilis2()
    # a=gsautilis.CheckCRAacc("CRA012526") #test CRAacc

    # a=gsautilis.CheckCRAid(197200) #test CRAid
    # a=gsautilis.CheckSampleACC("SAMC3045866") #测试sample acc
    # a=gsautilis.CheckExpACC("CRX782894a") #测试sample acc
    # a=gsautilis.CheckRunACC("CRR867241a") #测试sample acc
    # a=gsautilis.CRAaccRelatedID(["CRA012526","CRA004380"])
    # # a=gsautilis.CRAidRelatedID([19721,4840])
    # for i in a:
    #     print(i["cra_id"])

    # a=gsautilis.SAMCaccRelatedID(["SAMC3045861"])
    # a=gsautilis.CRXaccRelatedID(["CRX782894", "CRX782893"])
    # a=gsautilis.CRRaccRelatedID(["CRR867241", "CRR867240"])

    # a=gsautilis.prjidRelatedCRA(28843) #通过这个函数判断cra或者craacc下是否还关联了其他编号，判断prj_id是否应该撤回或者删除
    # a = gsautilis.CheckAcc("CRAacc", "CRR867241")
    # #
    # print(a)
    # a=["SAMC3045861","SAMC3045866","SAMC3045868"]

    # print(','.join(['"{}"'.format(item) for item in a]))


# def CheckId(idList):
#     res = []
#     for eachid in idList:
#         if eachid["type"] == "subCRA":
#             temp = gsautilis2.CheckAcc(eachid["type"], eachid["value"])
#             if temp["status"] == True:
#                 eachid["results"] = {"is_valid": True, "name": eachid["value"],
#                                      "modtime": temp["data"]["modify_time"].strftime("%Y-%m-%d %H:%M:%S"), "status": temp["data"]["status"]}
#                 res.append(eachid)
#             else:
#                 eachid["results"] = {
#                     "is_valid": False, "name": eachid["value"]}
#                 res.append(eachid)
#         else:
#             eachid["results"] = {"is_valid": gsautilis2.CheckAcc(
#                 eachid["type"], eachid["value"])["status"]}
#             res.append(eachid)
#     return res


# idList = [{'type': 'subCRA', 'value': 'subCRA019631'},
#           {'type': 'subCRA', 'value': 'subCRA019888'}]

# res = CheckId(idList)
# print(res)
