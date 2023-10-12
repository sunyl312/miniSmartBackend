
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from email import message
from email.mime.text import MIMEText
from collections import OrderedDict
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import html
import logging
import smtplib
import smtplib
import logging
import ssl
from UserPackage import UserPackage

public_root_dict = UserPackage("public_root_dict")
archive_root_dict = UserPackage("archive_root_dict")

# 定义一个类，并初始化值，初始化对类执行链接数据库的操作
from UserPackage import UserPackage

gsaloginfo = UserPackage("gsadb")
gsahost = gsaloginfo["username"]
gsaport = gsaloginfo["port"]
gsausername = gsaloginfo["username"]
gsapassword = gsaloginfo["password"]
gsadbname = gsaloginfo["dbname"]

class GSAutilis():
    def __init__(self):
        engine = create_engine(
            # f"mysql+pymysql://{gvar.mysql_username}:{gvar.mysql_pwd}@{gvar.mysql_host}:{gvar.mysql_port}/{gvar.mysql_dbname}?charset=utf8",
            f"mysql+pymysql://{gsausername}:{gsapassword}@{gsahost}:{gsaport}/{gsadbname}?charset=utf8",
            echo=True,  # echo ：为 True 时候会把sql语句打印出来
            max_overflow=999,  # 连接池的大小，为0表示无限制
            pool_pre_ping=True  # 这是1.2新增的参数，如果值为True，那么每次从连接池中拿连接的时候，都会向数据库发送一个类似 select 1 的测试查询语句来判断服务器是否正常运行。当该连接出现 disconnect 的情况时，该连接连同pool中的其它连接都会被回收。
        )
        # 通过sessionmake方法创建一个Session工厂，然后在调用工厂的方法来实例化一个Session对象
        self.Session = sessionmaker(bind=engine)

    def fail2back(self, cid):
        res = ['update experiment set status=2 where cra_id={};'.format(cid)]
        res.append(
            'update run_data_file set status=2 where cra_id={};'.format(cid))
        res.append('update run set status=2 where cra_id={};'.format(cid))
        # res1=['update experiment set status=2 where cra_id={};'.format(cid)]
        # res2='update run_data_file set status=2 where cra_id={};'.format(cid)
        # res3='update run set status=2 where cra_id={};'.format(cid)
        # res=res1 + '\n' +res2 + '\n' +res3
        return res

    def sample2back(self, sample_acc):
        if not sample_acc.startswith("subSAM"):
            return (False, "sample accession format is wrong!")
        if len(sample_acc) != 12:
            return (False, "sample accession length is not equal to 12!")

        res = [
            'update sample set status=2 where submissionId="{}";'.format(sample_acc)]
        res.append(
            'update sample_submission set status=2 where sample_submissionId="{}";'.format(sample_acc))
        return res

    def modify_status(self, cid, date, time):
        mod_time = str(date) + " " + str(time) + ':00'

        res = [
            'update run_data_file set status=10 where cra_id={} and status=11;'.format(cid)]
        res.append('update cra set status=2 where cra_id={};'.format(cid))
        res.append('update run set status=10,processed_success_time="{}" where cra_id={} and status=11;'.format(
            mod_time, cid))
        return res

    def call_back_by_acc(self, cra_acc):
        cra_acc = cra_acc.upper()

        if not cra_acc.startswith("CRA"):
            return (False, "cra accession format is wrong!")

        if len(cra_acc) != 9:
            return (False, "cra accession length is not equal to 9!")

        sql_fetch_cra = """
            SELECT
                status,
                release_state,
                prj_id,
                cra_id,
                archive_root,
                public_root
            FROM
                cra 
            WHERE
                accession ="{}";
        """.format(cra_acc)
        this_session = self.Session()
        # [ xx ] fetchone()  返回 dict
        res1 = this_session.execute(sql_fetch_cra).fetchall()

        prj_id = str(res1[0][2])
        cra_id = str(res1[0][3])
        archieve_root = archive_root_dict[str(res1[0][4])]
        public_root = public_root_dict[str(res1[0][5])]

        sql_fetch_cra_status_by_prj_id = """
	    SELECT
		    cra_id,
            status,
		    release_state
	    FROM
		    cra 
	    WHERE
		    prj_id ={} and cra_id!={} and release_state=2 and status!=5;
        """.format(prj_id, cra_id)  # 需要先处理prj_id,把res1中的prj_id取出

        res2 = this_session.execute(sql_fetch_cra_status_by_prj_id).fetchall()

        fetch_cra_id_by_sample_status = """
	    SELECT
		    distinct(cra_id)
	    FROM
		    experiment 
	    WHERE
		    sample_id in (select
            sample_id from experiment where cra_id ={}) and cra_id!={} and release_state=2 and status!=5;
        """.format(cra_id, cra_id)  # 需要先处理prj_id,把res1中的prj_id取出
        res3 = this_session.execute(fetch_cra_id_by_sample_status).fetchall()

        fetch_samplesub_id_by_craid = """
	    SELECT
		    distinct(submissionId)
	    FROM
		    sample 
	    WHERE
		    sample_id in (select
            sample_id from experiment where cra_id ={});
        """.format(cra_id)
        res4 = this_session.execute(fetch_samplesub_id_by_craid).fetchall()

        # return(True,cra_id,res2,res3,res4)

        return cra_id, prj_id, res2, res3, res4, archieve_root, public_root

    def call_back_by_cra_id(self, cra_id):
        # cra_id=cra_id

        # if type(cra_id)!=int:
        #     return (False,"cra id 不是数值!")

        sql_fetch_cra = """
            SELECT
                status,
                release_state,
                prj_id,
                accession,
                archive_root,
                public_root
            FROM
                cra 
            WHERE
                cra_id={};
        """.format(cra_id)
        this_session = self.Session()
        # [ xx ] fetchone()  返回 dict
        res1 = this_session.execute(sql_fetch_cra).fetchall()

        archieve_root = archive_root_dict[str(res1[0][4])]
        public_root = public_root_dict[str(res1[0][5])]

        prj_id = str(res1[0][2])
        cra_acc = str(res1[0][3])

        sql_fetch_cra_status_by_prj_id = """
	    SELECT
		    cra_id,
            status,
		    release_state
	    FROM
		    cra 
	    WHERE
		    prj_id ={} and cra_id!={} and release_state=2 and status!=5;
        """.format(prj_id, cra_id)  # 需要先处理prj_id,把res1中的prj_id取出

        res2 = this_session.execute(sql_fetch_cra_status_by_prj_id).fetchall()

        fetch_cra_id_by_sample_status = """
	    SELECT
		    distinct(cra_id)
	    FROM
		    experiment 
	    WHERE
		    sample_id in (select
            sample_id from experiment where cra_id ={}) and cra_id!={} and release_state=2 and status!=5;
        """.format(cra_id, cra_id)  # 需要先处理prj_id,把res1中的prj_id取出
        res3 = this_session.execute(fetch_cra_id_by_sample_status).fetchall()

        fetch_samplesub_id_by_craid = """
	    SELECT
		    distinct(submissionId)
	    FROM
		    sample 
	    WHERE
		    sample_id in (select
            sample_id from experiment where cra_id ={});
        """.format(cra_id)
        res4 = this_session.execute(fetch_samplesub_id_by_craid).fetchall()

        # return(True,cra_id,res2,res3,res4)
        return cra_acc, prj_id, res2, res3, res4, archieve_root, public_root

    def unsubmitted_id(self, statu, start_tim, end_tim):
        start_time = start_tim + " 00:00:00"
        end_time = end_tim + " 00:00:00"
        status_tem = statu.split("|")
        status = "(" + ",".join(status_tem) + ")"

        sql_fetch_cra = """
            SELECT
                cra_id
            FROM
                cra 
            WHERE
                status in {} and modify_time between "{}" and "{}";
        """.format(status, start_time, end_time)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_cra).fetchall()
        res_lis = []
        for i in res1:
            res_lis.append(str(i[0]))

        # sleep(2)
        return ",".join(res_lis)

    def auto_reminder(self, cra_id, subcra):
        reminder_sql = """
        select 	u.email,u.first_name,u.last_name,c.`status`,c.cra_id,c.user_id, c.create_time
        from cra c,user u 
        where c.cra_id={} and c.user_id=u.user_id;
        """.format(cra_id)
        this_session = self.Session()
        res = this_session.execute(reminder_sql).fetchall()

        usr_name = res[0]["last_name"]+" " + res[0]["first_name"]
        sedemail = str(res[0]["email"])
        sub_tim = res[0]["create_time"].isoformat().split("T")[0]
        msg = MIMEMultipart('alternative')
        sendimagefile = open('./图片1.png', "rb")
        image = MIMEImage(sendimagefile.read())
        sendimagefile.close()
        image.add_header("content-ID", "<image>")

        msg.attach(image)

        html = """
        <html>
            <head> </head>
            <body>
            <div>
            尊敬的GSA用户:{},<br/>
            您于{}提交的数据({})元信息还没有完成提交。未完成提交的元信息管理员无法查看审核。<br/><br/>
            如果不需要了，数据文件和元信息请及时删除！<br/><br/>
            如果还需要，请登录数据库继续完成提交(元信息和数据文件上传可以同时进行，无需等数据文件传完后再提交元信息)。数据提交完成后，我们会在工作日当天审核。<br/>
            <br/><br/><br/>
            备注：元信息审核通过后，后台数据是每1-2小时关联审核一次。您可以点页面的detail自行检查审核问题，并根据提示修改！查看及修改方式如图所示：<br/> 
            </div>
            
                <img src="cid:image"></img>

            <div>
            <br/> <br/> -----------------------------------------------------------------------------------------------------<br/> <br/> <br/> 
            祝好，<br/> 
            GSA-工作组
            </div>
            </body>
        </html>
        """.format(
            usr_name,
            sub_tim,
            subcra,
        )
        message = MIMEText(html, 'html', 'utf-8')
        msg.attach(message)
        return sedemail, msg

    def makeSubCRA(self, craid):
        s = str(craid)
        l = 6
        sl = len(s)
        add0 = l - sl
        subcra = "subCRA" + "0"*add0 + s
        return subcra

    def auto_mail(self, this_message, this_subject, t_addr, cc_addr):
        f_addr = "sunyanling@big.ac.cn"
        f_pswd = "8Ge~AdHVpv8WmaDI"
        f_smtp = "mail.cstnet.cn"
        f_cc = "gsa@big.ac.cn"

        # msg = MIMEText(this_message,'html','utf-8')
        msg = this_message
        msg['Subject'] = this_subject
        msg['From'] = f_addr
        msg['To'] = ",".join(t_addr) if isinstance(t_addr, list) else t_addr
        msg['Cc'] = ",".join(cc_addr) if isinstance(cc_addr, list) else cc_addr

        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        # Either of the following context settings worked for me - choose one
        context.set_ciphers('HIGH:!DH:!aNULL')
        # context.set_ciphers('DEFAULT@SECLEVEL=1')
        # context.set_ciphers('DEFAULT:!DH')
        # context.set_ciphers('DEFAULT:!DH:!kRSA')
        server = smtplib.SMTP_SSL(f_smtp, 465, context=context)
        server.set_debuglevel(0)
        server.login(f_addr, f_pswd)
        server.sendmail(f_addr, t_addr + cc_addr, msg.as_string())
        server.quit()
        logging.info(
            "auto mailing... From:{0} ; To:{1}".format(f_addr, msg['To']))

# gsautilis.auto_mail(msg,"GSA {}元信息未提交提醒".format(subcra),[sedemail],["gsa@big.ac.cn"])

    def archieve_status_checking(self, cra_id_list, status):
        sql_fetch_status = """
            SELECT
                cra_id,modify_time,status
            FROM
                cra 
            WHERE
                status = {} and cra_id in ({});
        """.format(status, cra_id_list)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_status).fetchall()
        return res1

    def del_fetch_cra_id_by_cra_accession(self, cra_acc):
        sql_fetch_craacc = """
            SELECT
                status,
                prj_id,
                cra_id,
                release_state,
                archive_root,
                public_root
            FROM
                cra 
            WHERE
                accession  in ({});
        """.format(cra_acc)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_craacc).fetchall()
        return (True, res1)

    def del_fetch_cra_status_by_prj_id(self, prj_id, cra_id):
        sql_fetch_prj = """
	    SELECT
		    cra_id,
            status
	    FROM
		    cra 
	    WHERE
		    prj_id in ({}) and cra_id not in ({}) and status!=5;
        """.format(prj_id, cra_id)
        this_session = self.Session()
        res2 = this_session.execute(sql_fetch_prj).fetchall()
        return (True, res2)

    def del_fetch_cra_id_by_sample_status(self, cra_id):
        sql_fetch_cra = """
	    SELECT
		    distinct(cra_id)
	    FROM
		    experiment 
	    WHERE
		    sample_id in (select
            sample_id from experiment where cra_id in ({})) and cra_id not in ({}) and status!=5;
        """.format(cra_id, cra_id)  # 确定这个样本集是不是关联了其他的数据集
        this_session = self.Session()
        res3 = this_session.execute(sql_fetch_cra).fetchall()

        return (True, res3)

    def del_fetch_samplesub_id_by_craid(self, cra_id):
        fetch_samplesub_id_by_craid = """
	    SELECT
		    distinct(submissionId)
	    FROM
		    sample 
	    WHERE
		    sample_id in (select
            sample_id from experiment where cra_id  in ({}));
        """.format(cra_id)
        this_session = self.Session()
        res4 = this_session.execute(fetch_samplesub_id_by_craid).fetchall()
        return (True, res4)

    def del_fetch_cra_by_cra_id(self, cra_id):
        sql_fetch_craacc = """
            SELECT
                status,
                prj_id,
                accession,
                release_state
            FROM
                cra 
            WHERE
                cra_id  in ({});
        """.format(cra_id)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_craacc).fetchall()
        return (True, res1)

    def del_fetch_sample_by_sample_acc(self, sample_list):
        sql_fetch_sample_acc = '''
        select sample_id,submissionId from sample where accession in ({});'''.format(sample_list)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_sample_acc).fetchall()
        return (True, res1)

    def del_fetch_sample_by_samplesubmissionID(self, sample_submissionID, sample_list):
        sql_fetch_sample_by_samplesubmissionID = '''
        select sample_id from sample where submissionId in ({}) and sample_id not in ({});'''.format(sample_submissionID, sample_list)
        this_session = self.Session()
        res2 = this_session.execute(
            sql_fetch_sample_by_samplesubmissionID).fetchall()
        return (True, res2)

    def del_fetch_cra_run_by_sample_id(self, sample_list):
        sql_fetch_run_by_sample_id = '''
        select c.cra_id,c.accession as cra_accession,c.status,c.release_state,r.accession,c.archive_root,c.public_root from cra c,run r where r.sample_id in ({}) and r.cra_id=c.cra_id;'''.format(sample_list)
        this_session = self.Session()
        res2 = this_session.execute(sql_fetch_run_by_sample_id).fetchall()

        # for row in res2:
        #     return dict(row)
        return (True, res2)

    def del_fetch_exp_by_experiment_acc(self, experiment_list):
        sql_fetch_exp_by_experiment_acc = '''
        select exp_id,sample_id from experiment where accession in ({});'''.format(experiment_list)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_exp_by_experiment_acc).fetchall()
        return (True, res1)

    def del_fetch_sample_by_exp_id(self, expid_list):
        sql_fetch_sample_by_exp_id = '''
        select exp_id from experiment where sample_id in (select sample_id from experiment where exp_id in ({})) and exp_id not in ({});
        '''.format(expid_list, expid_list)
        this_session = self.Session()
        res2 = this_session.execute(sql_fetch_sample_by_exp_id).fetchall()
        return (True, res2)

    def del_fetch_cra_run_by_experiment_id(self, experiment_list):
        sql_fetch_cra_run_by_experiment_id = '''
        select c.cra_id,c.accession as cra_accession,c.status,c.release_state,r.accession,c.archive_root,c.public_root from cra c,run r where r.exp_id in ({}) and r.cra_id=c.cra_id;'''.format(experiment_list)
        this_session = self.Session()
        res2 = this_session.execute(
            sql_fetch_cra_run_by_experiment_id).fetchall()

        # for row in res2:
        #     return dict(row)
        return (True, res2)

    def del_fetch_run_by_run_acc(self, run_list):
        sql_fetch_run_by_run_acc = '''
        select run_id,exp_id,sample_id from run where accession in ({});'''.format(run_list)
        this_session = self.Session()
        res1 = this_session.execute(sql_fetch_run_by_run_acc).fetchall()
        return (True, res1)

    def del_fetch_exp_by_run_id(self, runid_list):
        sql_fetch_exp_by_run_id = '''
        select run_id from run where exp_id in (select exp_id from run where run_id in ({})) and run_id not in ({});
        '''.format(runid_list, runid_list)
        this_session = self.Session()
        res2 = this_session.execute(sql_fetch_exp_by_run_id).fetchall()
        return (True, res2)

    def del_fetch_sample_by_run_id(self, runid_list):
        sql_fetch_sample_by_run_id = '''
        select run_id from run where sample_id in (select sample_id from run where run_id in ({})) and run_id not in ({});
        '''.format(runid_list, runid_list)
        this_session = self.Session()
        res3 = this_session.execute(sql_fetch_sample_by_run_id).fetchall()
        return (True, res3)

    def del_fetch_cra_run_by_run_id(self, run_list):
        sql_fetch_cra_run_by_run_i = '''
        select c.cra_id,c.accession as cra_accession,c.status,c.release_state,r.accession,c.archive_root,c.public_root from cra c,run r where r.run_id in ({}) and r.cra_id=c.cra_id;'''.format(run_list)
        this_session = self.Session()
        res2 = this_session.execute(sql_fetch_cra_run_by_run_i).fetchall()
        return (True, res2)

    def fetch_journal(self):
        sel_fetch_journal = '''
        select distinct(journal_title) from publication where journal_id is null and gsa_accession is not null and is_deleted_by_user=0;
        '''
        this_session = self.Session()
        res = this_session.execute(sel_fetch_journal).fetchall()
        return (True, res)


if __name__ == "__main__":
    gsa = GSAutilis()
    # res=gsa.fail2back(12111)
    # print(res[1])

    # ress=gsa.call_back_by_acc("CRA008459")
    # ress=gsa.call_back_by_cra_id(13045)
    # cra_acc=ress[0]
    # prj_id=ress[1]
    # sample_submissionID=ress[4][0][0]

    # print(sample_submissionID)
    # status="14|15".split("|")

    # # status1=
    # # status2="(" + ",".join(status) +")"
    # # print(status2)

    # res=gsa.unsubmitted_id("14|15","2022-01-08","2022-10-16")
    # print(res)
    # cra_id=11542

    # subcra=gsa.makeSubCRA(cra_id)
    # # print(subcra)
    # emal_mess=gsa.auto_reminder(cra_id,subcra)
    #     # sedemail=emal_mess[0]
    # msg=emal_mess[1]
    #     # gsautilis.auto_mail(msg,"GSA {}元信息未提交提醒".format(subcra),[sedemail],["gsa@big.ac.cn"])
    # # print(msg)

    # gsa.auto_mail(msg,"GSA {}元信息未提交提醒".format(subcra),["sunyanling@big.ac.cn"],["sunyanling@big.ac.cn"])
    # cra_id="subCRA012561,subCRA012679,subCRA013030,subCRA013073,subCRA012561,subCRA013071,subCRA013064,"
    # cra_id=cra_id.replace("subCRA","").strip(",")
    # res=gsa.archieve_status_checking(cra_id,3)
    # print(res)
    # ressss =[]
    # for i in res:
    #     tem={}
    #     tem["checking_cra_id"]=i[0]
    #     tem['modify_time']=i[1]
    #     ressss.append(tem)

    # print(ressss)
    res = gsa.fetch_journal()
    journal = []
    for i in res[1]:
        journal.append(i[0])
    print(journal)
