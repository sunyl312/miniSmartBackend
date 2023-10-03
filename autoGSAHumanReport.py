#!/usr/bin/env python3

from Utilis import FTPUtilis
from UserPackage import UserPackage
from cmath import log
import pymysql
import os
import argparse
import logging
import json
import smtplib
from email.mime.text import MIMEText
from collections import OrderedDict
import paramiko
from autoMail import auto_mail
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
logging.basicConfig(level=logging.INFO)


userinfo = UserPackage("gsahumandb")
ftpinfo = UserPackage("ftp21")["login"]

GSA_process_path = UserPackage("GSAHuman_process_path")


class config():
    sender_email_addr = ""
    sender_email_passwd = ""

    err_base_dir = ""

    db_host = userinfo["host"]
    db_port = userinfo["port"]
    db_name = userinfo["dbname"]
    db_username = userinfo["username"]
    db_passwd = userinfo["password"]
    db_charset = "utf8"



    error_messages = {
        "3_23": """以下文件没有找到：请检查文件是否上传到GSA目录下，文件名和元信息中填写是否一致。如果不一致可以登陆数据库点run编号后的修改更新或重命名数据文件。如果您通过邮寄硬盘的方式上传数据或正在自行上传数据，请忽略此邮件提醒。
        """,
        "3_22": """以下文件md5不一致：请检查md5填写是否有误。填写有误请登录数据库点run后面的修改在线更新。如果填写无误，说明是文件上传有问题导致，请选择二进制重新上传文件（Filezilla: 传输——传输类型——二进制）。
        """,
        "11_32": """以下文件审核报错，原因为xxxx, 请核查数据情况，去除dup或查找正确格式的数据后重新上传数据，并登陆数据库点run编号后的修改在线更新数据md5。或者也可以上传mapping后的bam文件。
        """,
        "2_32": """以下文件审核报错，原因为xxxx, 请核查数据情况，去除dup或查找正确格式的数据后重新上传数据，并登陆数据库点run编号后的修改在线更新数据md5。或者也可以上传mapping后的bam文件。
        """
    }

def isEmpty(err_path):
    if os.stat(err_path).st_size == 0:
        return True
    else:
        return False


class mysqlUtils():

    def __init__(self):

        self.host = config.db_host
        self.port = config.db_port
        self.username = config.db_username
        self.password = config.db_passwd
        self.dbname = config.db_name
        self.charsets = config.db_charset

        self.base_header = "INSERT INTO {}.".format(self.dbname)
        try:
            self.con = pymysql.Connect(
                host=self.host,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.dbname,
                charset=self.charsets,
            )
            # 获得数据库的游标
            self.cursor = self.con.cursor(
                cursor=pymysql.cursors.DictCursor)  # 开启事务
            logging.info("Get cursor successfully")
        except Exception as e:
            logging.info(
                "Can not connect databse {}\nReason:{}".format(self.dbname, e))

    def close(self):
        if self.con:
            self.con.commit()
            self.con.close()
            logging.info("Close database {} successfully".format(self.dbname))
        else:
            logging.info(
                "DataBase doesn't connect,close connectiong error;please check the db config.")

    def fetchOne(self):
        self.con.ping(reconnect=True)
        data = self.cursor.fetchone()
        return (data)

    def fetchAll(self):
        self.con.ping(reconnect=True)
        data = self.cursor.fetchall()
        return (data)

    def excute(self, sql, args=None):

        if args == None:
            logging.debug(sql)
            # logger.info(repr(sql))
            self.con.ping(reconnect=True)
            self.cursor.execute(sql)
            return (self.cursor.rowcount)
        else:
            logging.debug(sql)
            logging.debug(str(args))
            # logger.info(repr(sql))
            self.con.ping(reconnect=True)
            self.cursor.execute(sql, args)
            return (self.cursor.rowcount)


def makeSubHRA(studyid):
    s = str(studyid)
    l = 6
    sl = len(s)
    add0 = l - sl
    return "subHRA" + "0"*add0 + s


def autoGSAHumanReport(studyid, email=None):
    try:
        dbutil = mysqlUtils()

        sql = """
        SELECT `user`
        .email,
        `user`.first_name,
        `user`.last_name,
        a.`status`,
        a.study_id,
        a.user_id,
        a.run_id,
        a.run_file_name,
        a.archived_file_dir,
        a.process_status 
        FROM
        (
        SELECT
            run_data_file.`status`,
            study_id,
            user_id,
            run_id,
            run_file_name,
            archived_file_dir,
            run_data_file.process_status 
        FROM
            run_data_file 
        WHERE
            study_id ={} and ( run_data_file.`status` = 3 OR run_data_file.`status` = 11 OR run_data_file.process_status = 32 )  and  (run_data_file.`status` != 5 )
        ORDER BY
            study_id DESC 
        ) AS a
        LEFT JOIN user ON a.user_id = user.user_id;
        """.format(studyid)
        dbutil.excute(sql)

        res = list(dbutil.fetchAll())
        if len(res) == 0:
            return {"status": "error", "data": "请核查数据是正在运行或未审核！"}
        # print(res)

        record = OrderedDict()
        # print(res)
        for x in res:
            if x['study_id'] not in record.keys():
                statusKey = str(x['status']) + "_" + str(x['process_status'])
                record[x['study_id']] = {
                    "study_id": x['study_id'],
                    "user_id": x['user_id'],
                    "run_id": x["run_id"],
                    'first_name': x['first_name'],
                    'last_name': x['last_name'],
                    'email': x['email'],
                    "status": {
                        statusKey: {
                            "run_file_name": [
                                {
                                    "file": x['run_file_name'],
                                    "archive_dir":x["archived_file_dir"],
                                    "run_id":x["run_id"]
                                }

                            ]
                        }
                    }
                }
            else:
                statusKey = str(x['status']) + "_" + str(x['process_status'])
                if statusKey not in record[x['study_id']]["status"].keys():
                    record[x['study_id']]["status"][statusKey] = {
                        "run_file_name": [
                            {
                                "file": x['run_file_name'],
                                "archive_dir":x["archived_file_dir"],
                                "run_id":x["run_id"]
                            }
                        ]
                    }
                else:
                    record[x['study_id']]["status"][statusKey]["run_file_name"].append({
                        "file": x['run_file_name'],
                        "archive_dir": x["archived_file_dir"],
                        "run_id": x["run_id"]
                    })
        print(record)

        total_msg = []
        if len(record.values()) == 0:
            raise Exception(
                f"can not found any status code [{ ', '.join( list(config.error_messages.keys())) }] for {makeSubHRA(studyid)}.")
        for x in record.values():

            msg = MIMEMultipart('alternative')

            sendimagefile = open('./GSAHuman-邮件反馈.png', "rb")
            image = MIMEImage(sendimagefile.read())
            sendimagefile.close()
            image.add_header("content-ID", "<image>")
            msg.attach(image)

            subMsg = []
            count = 0
            ftputilis = FTPUtilis()
            for k, m in x["status"].items():

                print(f"Found status: {k} for {makeSubHRA(studyid)}")

                if k.strip().startswith("3"):
                    count += 1
                    the_msg = config.error_messages.get(
                        k, "Not Found Description!")
                    the_msg = "\n" + str(count)+". " + the_msg + "\n" + \
                        "\n".join([f['file'] for f in m["run_file_name"]])
                    subMsg.append(the_msg)
                elif k.strip() == "11_32" or k.strip() == "2_32":
                    count += 1
                    run_id_dict = {}
                    # 获得 runid 和 ahrive id 对应关系 并且去重复
                    for onefileinfo in m["run_file_name"]:
                        if onefileinfo['run_id'] not in run_id_dict.keys():
                            run_id_dict[onefileinfo['run_id']
                                        ] = onefileinfo['archive_dir']

                    the_msg = config.error_messages.get(
                        k, "Not Found Description!")
                    the_msg = "\n" + str(count)+". " + the_msg + "\n"
                    for runid, archivedir in run_id_dict.items():

                        CRRDIR = os.path.split(archivedir)[0]

                        for v in GSA_process_path.values():
                            path_temp = v+CRRDIR
                            # print(path_temp)
                            aaa = ftputilis.exec(f"ls {path_temp}")
                            if len(aaa) != 0:
                                GSA_file_process_basedir = v
                        # err_file2="{}{}".format("/p300/gsafileprocess/process",os.path.join(CRRDIR,str(runid)+".2.err"))

                        cmd = "cat {}/{}".format(GSA_file_process_basedir,
                                                 os.path.join(CRRDIR, "hra_"+str(runid)+".2.err"))
                        cmd2 = "cat {}/{}".format(GSA_file_process_basedir,
                                                  os.path.join(CRRDIR, "hra_"+str(runid)+".err"))
                        cmd3 = "cat {}/{}".format(GSA_file_process_basedir,
                                                  os.path.join(CRRDIR, str(runid)+".2.err"))
                        cmd4 = "cat {}/{}".format(GSA_file_process_basedir,
                                                  os.path.join(CRRDIR, str(runid)+".err"))
                        print(cmd, cmd2, cmd3, cmd4)

                        try:
                            res1 = ftputilis.exec(cmd)
                            res2 = ftputilis.exec(cmd2)
                            res3 = ftputilis.exec(cmd3)
                            res4 = ftputilis.exec(cmd4)
                            if len(res1) != 0:
                                res = res1
                            elif len(res2) != 0:
                                res = res2
                            elif len(res3) != 0:
                                res = res3
                            elif len(res4) != 0:
                                res = res4
                            print(res)

                            cleanres = CRRDIR.split(
                                "/")[2] + " : " + str(res).replace("/p300/HRA-Process/temp"+CRRDIR, "").replace("/hracond3/HRA-Process/temp"+CRRDIR, "").replace("/hracond2/HRA-Process/temp"+CRRDIR, "").strip()
                            # print(cleanres)
                            the_msg += "\n" + cleanres
                        except Exception as e:
                            print("WARN: get file error {} failed!\n reason: {}".format(
                                cmd, str(e)))

                    subMsg.append(the_msg)
                    pass
            html = """
            <html>
                <head> </head>
                <body>
                    <div>
                        用户Email： {}<br/>**********************<br/>尊敬的GSA用户:{} {},<br/>您的提交数据({})问题如下：<br/><br/>
                        <div>
                            <pre>
                            {}
                            <pre>
                        </div>
                    </div>
                    <br/>
                    备注：元信息审核通过后，后台数据是每1-2小时关联审核一次（不包含每周工作日第一天或节假日第一天）。您可以点页面的detail自行检查审核问题，并根据提示修改！查看及修改方式如图所示：
                    <br/>
                    <img src="cid:image" width="1200" height="800"></img>
                    <br/> -----------------------------------------------------------------------------------------------------<br/> <br/> <br/> 
                    祝好，<br/> 
                    GSA-工作组
                    </div>
                </body>
            </html>
            """.format(
                x['email'],
                x['last_name'],
                x['first_name'],
                makeSubHRA(x['study_id']),
                "\n".join(subMsg)
            )
            message = MIMEText(html, 'html', 'utf-8')
            logging.debug(html)
            msg.attach(message)
            print(subMsg)
            # msg += "\n" +"\n".join(subMsg)
            # total_msg.append(msg)
            # print("aa")
            if email == None:
                email = "sunyanling@big.ac.cn"

            email = f"{email}"

            # try:

            auto_mail(msg, "GSAHuman {}提交问题反馈".format(
                makeSubHRA(x['study_id'])), email)
            # except Exception as e:
            #     print(e)

            # print("\n".join(total_msg))
            ftputilis.close()
        return {"status": "ok", "data": {"studyid": makeSubHRA(studyid=studyid)}}
    except Exception as e:
        print(e)
        return {"status": "error", "data": str(e)}


if __name__ == "__main__":

    # parser = argparse.ArgumentParser()
    # parser.add_argument("-c", "--craid", help="cra id")
    # args = parser.parse_args()
    # status = autoGSAReport(args.craid)
    # print(status)
    # # sshExeCMD()
    # userinfo = UserPackage("gsadb")
    # print(userinfo)
    # autoGSAHumanReport(7758)
    autoGSAHumanReport(6151)
    # autoGSAHumanReport(4385)
