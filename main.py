from UserPackage import UserPackage
from fastapi import FastAPI, File, UploadFile, HTTPException
from typing import List, Optional, Union
from GSAManager import GSAutilis
from sqlalchemy import create_engine
import json
from autoGSAReport import autoGSAReport
from autoGSAHumanReport import autoGSAHumanReport
from MapPublication import Map_Publication
from GSA2xml import generate_by_craacc
import logging
from starlette.responses import FileResponse
import os
from Utilis import GSAUtilis2, UUIDgeneration, path_generate_for_email_prjacc, path_generate
from typing import List, Dict
from datetime import datetime
from fastapi.responses import JSONResponse
import asyncio
import time
from autoMail import auto_mail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import GSA2SRA


from GSAManager import GSAutilis
gsautilis = GSAutilis()
map_pub = Map_Publication()

app = FastAPI()

public_root_dict = UserPackage("public_root_dict")
archive_root_dict = UserPackage("archive_root_dict")

gsautilis2 = GSAUtilis2()

@app.post("/api/refresh_status/", tags=["minismartV2"])
def refresh_status(idList: Dict):
    print(idList)
    page_number=idList["page_number"]
    page_size=idList["page_size"]
    sort_value=idList["sort_value"]
    sort_type=idList["sort_type"]
    print({"sort_value":sort_value})
    result=idList
    res=GSA2SRA.getStatusByCRAacc(page_number,page_size,sort_value,sort_type)
    result["result"]=res
    return result


@app.post("/api/scanGSAtable/", tags=["minismartV2"])
def scanGSAtable():
    res = GSA2SRA.InsertData2Sqlite()
    return res


@app.post("/api/getRelatedID/", tags=["minismartV2"])
def getRelatedID(idList: List):
    ress = []
    for eachid in idList:
        res = gsautilis2.CheckRelatedID(eachid["type"], eachid["value"])
        if res["status"] == True:
            eachid["status"] = True
            if eachid["type"] == "PRJacc" or eachid["type"] == "email":
                eachid["relatedid"] = path_generate_for_email_prjacc(res)
            else:
                eachid["relatedid"] = path_generate(res)
        else:
            eachid["status"] = False
        ress.append(eachid)
    return ress


@app.post("/api/taxon2gsa/", tags=["minismartV2"])
def taxon2gsa(taxontxt: UploadFile):
    # try:
    uid = str(UUIDgeneration())
    current_time = str(time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime()))
    uid_wkspace = os.path.join(
        "/workspace/project/GSA/taxon2gsa", current_time + " " + uid).replace(" ", "_").replace(":", "_")
    os.mkdir(uid_wkspace)
    with open(os.path.join(uid_wkspace, uid+".txt"), "wb+") as outf:
        outf.write(taxontxt.file.read())
        input = os.path.join(uid_wkspace, uid+".txt").replace(' ', r'\ ')
        out_config = os.path.join(uid_wkspace, uid).replace(' ', r'\ ')
        error = os.path.join(out_config+".err").replace(' ', r'\ ')
        sql = os.path.join(out_config+".sql").replace(' ', r'\ ')
        print(input, out_config, error, sql)
        command = f"python3 tax2gsa.py -i {input} -r ./gcode_ref_2023-07-18  -o {out_config}"
        print(command)
    if os.path.exists(input):
        os.system(command)
        if os.path.getsize(error) != 0:
            with open(error, "r") as file:
                file_content = file.read()
                return {"status": False, "message": file_content, "uuid": current_time + " " + uid, "content": ""}
        else:
            with open(sql, "r") as file:
                file_content = file.read()
                subject = current_time + " add species for " + uid
                msg = MIMEMultipart('alternative')
                text_part = MIMEText(file_content, 'plain')  # 使用纯文本格式
                msg.attach(text_part)
                auto_mail(msg, subject,
                          "sunyanling@big.ac.cn")
                return {"status": True, "message": "上传的物种lineage文件校验通过，GSA管理员会添加到GSA数据库中，请耐心等待。", "uuid": current_time + " " + uid, "content": file_content}

    else:
        return {"status": False, "message": "后台读取文件失败", "uuid": current_time + " " + uid, "content": ""}
    # except:
    #     return {"status": False, "message": "获取文件失败", "uuid": current_time + " " + uid, "content": ""}


@app.post("/api/GSA2XMLFile/", tags=["minismartV2"])
def GSA2XMLFile(idList: List):
    # idList = [{"type": "CRAacc", "value": "CRA004967"}]
    res = []
    for eachid in idList:
        cra_lis = eachid["value"]
        stats = generate_by_craacc(cra_lis)
        if stats == True:
            filename = '{}_submission.xml'.format(cra_lis)
            join_result = os.path.join(
                "/workspace/project/GSA/GSA_XML/", filename)

            with open(join_result, "r") as file:
                file_content = file.read()
                eachid["status"] = True
                eachid["content"] = file_content
                res.append(eachid)

        else:
            eachid["status"] = False
            res.append(eachid)
    return res


@app.post("/api/AssistModifyStatus/", tags=["minismartV2"])
def AssistModifyStatus(idList: List):
    [
        {"type": "subCRA", "value": "subCRA012111",
            "moditytime": "2023-09-10 00:00:00", "modifystatus": "10"},
        {"type": "subCRA", "value": "subCRA012222",
            "moditytime": "2023-09-10 00:00:00", "modifystatus": "10"}
    ]
    ress = []
    for eachid in idList:

        temp = gsautilis2.CheckRunStatus(eachid["value"].replace("subCRA", ""))
        if "11" in temp:
            eachid["myqsql"] = []
            eachid["status"] = True
            eachid["myqsql"].extend(['update run_data_file set status={} where cra_id={} and status=11;'.format(eachid["modifystatus"], eachid["value"].replace("subCRA", "")),
                                    'update run set status={},processed_success_time="{}" where cra_id={} and status=11;'.format(
                                        eachid["modifystatus"], eachid["moditytime"], eachid["value"].replace("subCRA", "")),
                                     'update cra set status=2 where cra_id={};'.format(eachid["value"].replace("subCRA", ""))])
            ress.append(eachid)

        else:
            eachid["status"] = False
            ress.append(eachid)
    return ress


@app.post("/api/DeleteGSA/", tags=["minismartV2"])
def DeleteGSA(idList: List):

    ress = []
    for eachid in idList:
        eachid["mysql"] = []
        eachid["ftp"] = []

        cra_id_list = ",".join(
            ['"' + i.replace("subCRA", "") + '"' for i in eachid["value"]])

        # cra_id_list = cra_id_list.replace("subCRA", "")

        try:
            if eachid["type"] == "CRAacc":
                res1 = gsautilis.del_fetch_cra_id_by_cra_accession(cra_id_list)
                prj_temp = []
                cra_temp = []
                sample_submissionID = []

                if not res1[0]:
                    print(res1[1])
                else:
                    for i in res1[1]:
                        # print(str(i[1]), type(str(i[1])))
                        prj_temp.append(str(i[1]))
                        cra_temp.append(str(i[2]))
                    # print(prj_temp)

                    prj_id = ",".join(prj_temp)
                    cra_id = ",".join(cra_temp)
                res2 = gsautilis.del_fetch_cra_status_by_prj_id(prj_id, cra_id)
                res3 = gsautilis.del_fetch_cra_id_by_sample_status(cra_id)

                res4 = gsautilis.del_fetch_samplesub_id_by_craid(cra_id)

                for i in res4[1]:

                    sample_submissionID.append(i[0])
                    sample_submissionID1 = ','.join(
                        ['"' + i + '"' for i in sample_submissionID])

                eachid["mysql"].append(
                    'update cra set status=5 where cra_id in ({});'.format(cra_id))
                eachid["mysql"].append(
                    'update experiment set status=5 where cra_id in ({});'.format(cra_id))
                eachid["mysql"].append(
                    'update run set status=5 where cra_id in ({});'.format(cra_id))
                eachid["mysql"].append(
                    'update run_data_file set status=5 where cra_id in ({});'.format(cra_id))

                if len(res2[1]) == 0:
                    eachid["mysql"].append(
                        'update project set status=5 where prj_id in ({});'.format(prj_id))
                else:
                    pass

                if len(res3[1]) == 0:
                    eachid["mysql"].append(
                        'update sample set status=5 where submissionId in ({});'.format(sample_submissionID1))
                    eachid["mysql"].append(
                        'update sample_submission set status=5 where sample_submissionId in ({});'.format(sample_submissionID1))
                else:
                    pass

                # #数据删除

                for i in cra_id_list.split(","):

                    res5 = gsautilis.del_fetch_cra_id_by_cra_accession(i)
                    i = i.strip("\"")
                    if res5[1][0][0] == 3 and res5[1][0][3] == 2:
                        eachid["ftp"].append(
                            'rm -r {}/{}'.format(public_root_dict[str(res5[1][0][5])], i))

                    elif res5[1][0][0] == 3 and res5[1][0][3] == 1:
                        eachid["ftp"].append(
                            'rm -r {}/{}'.format(archive_root_dict[str(res5[1][0][4])], i))

                    elif res5[1][0][0] == 4 or res5[1][0][0] == 2:
                        eachid["ftp"].append(
                            'rm -r /p300/gsafileprocess/temp/{}'.format(i))
                    eachid["status"] = True

                ress.append(eachid)

            elif eachid["type"] == "subCRA":
                res1 = gsautilis.del_fetch_cra_by_cra_id(cra_id_list)

                prj_temp = []
                cra_acc_temp = []
                sample_submissionID = []

                if not res1[0]:
                    print(res1[1])
                else:
                    for i in res1[1]:
                        print(str(i[1]), type(str(i[1])))
                        prj_temp.append(str(i[1]))
                        cra_acc_temp.append(str(i[2]))

                    prj_id = ",".join(prj_temp)
                    cra_acc = ",".join(cra_acc_temp)
                    print(cra_acc, prj_id)
                res2 = gsautilis.del_fetch_cra_status_by_prj_id(
                    prj_id, cra_id_list)
                res3 = gsautilis.del_fetch_cra_id_by_sample_status(cra_id_list)

                res4 = gsautilis.del_fetch_samplesub_id_by_craid(cra_id_list)

                for i in res4[1]:

                    sample_submissionID.append(i[0])
                sample_submissionID1 = ','.join(
                    ['"' + i + '"' for i in sample_submissionID])
                print(sample_submissionID1)

                eachid["mysql"].append(
                    'update cra set status=5 where cra_id in ({});'.format(cra_id_list))
                eachid["mysql"].append(
                    'update experiment set status=5 where cra_id in ({});'.format(cra_id_list))
                eachid["mysql"].append(
                    'update run set status=5 where cra_id in ({});'.format(cra_id_list))
                eachid["mysql"].append(
                    'update run_data_file set status=5 where cra_id in ({});'.format(cra_id_list))

                if len(res2[1]) == 0:
                    eachid["mysql"].append(
                        'update project set status=5 where prj_id in ({});'.format(prj_id))
                else:
                    pass

                if len(res3[1]) == 0:
                    eachid["mysql"].append(
                        'update sample set status=5 where submissionId in ({});'.format(sample_submissionID1))
                    eachid["mysql"].append(
                        'update sample_submission set status=5 where sample_submissionId in ({});'.format(sample_submissionID1))
                else:
                    pass

                # # #数据删除

                for i in cra_acc.split(","):

                    res5 = gsautilis.del_fetch_cra_id_by_cra_accession(
                        ("\"" + i + "\""))
                    if res5[1][0][0] == 3 and res5[1][0][3] == 2:
                        eachid["ftp"].append(
                            'rm -rf {}/{}'.format(public_root_dict[str(res5[1][0][5])], i))

                    elif res5[1][0][0] == 3 and res5[1][0][3] == 1:
                        eachid["ftp"].append(
                            'rm -rf {}/{}'.format(archive_root_dict[str(res5[1][0][4])], i))

                    elif res5[1][0][0] == 4 or res5[1][0][0] == 2:
                        eachid["ftp"].append(
                            'rm -rf /p300/gsafileprocess/temp/{}'.format(i))
                    eachid["status"] = True
                ress.append(eachid)

            elif eachid["type"] == "SAMCacc":
                res1 = gsautilis.del_fetch_sample_by_sample_acc(cra_id_list)
                # print(type(res1[1]))
                sample_list = []
                sample_submissionID = []
                for i in res1[1]:
                    # print(i[1],type(i[1]))
                    sample_list.append(i[0])
                    sample_submissionID.append("\"" + str(i[1]) + "\"")
                # print(set(sample_submissionID))
                # print(sample_list,type(sample_list))
                s_lis = ",".join(str(i) for i in sample_list)
                # print(s_lis)

                eachid["mysql"].append(
                    'update sample set status=5 where sample_id in ({});'.format(s_lis))
                eachid["mysql"].append(
                    'update experiment set status=5 where sample_id in ({});'.format(s_lis))
                eachid["mysql"].append(
                    'update run set status=5 where sample_id in ({});'.format(s_lis))
                eachid["mysql"].append(
                    'update run_data_file set status=5 where sample_id in ({});'.format(s_lis))
                # print(ressss['mysql'])

                sample_submissionID1 = ",".join(str(i)
                                                for i in set(sample_submissionID))
                for i in sample_submissionID:
                    # print(i)
                    res2 = gsautilis.del_fetch_sample_by_samplesubmissionID(
                        i, s_lis)
                    # print(res2[1])
                    if len(res2[1]) == 0:
                        eachid["mysql"].append(
                            'update sample_submission set status=5 where sample_submissionId in ({});'.format(i))
                    else:
                        pass
                res3 = gsautilis.del_fetch_cra_run_by_sample_id(s_lis)
                print(res3)

                del_table = {}
                for i in res3[1]:
                    tempres = {y: z for y, z in i._asdict().items()}
                    print(tempres)
                    if tempres['cra_accession'] not in del_table.keys():
                        del_table[tempres['cra_accession']] = tempres
                        del_table[tempres['cra_accession']]['accession'] = [
                            tempres['accession']]
                    else:
                        del_table[tempres['cra_accession']]['accession'].append(
                            tempres['accession'])
                print(json.dumps(del_table, indent=4))

                for v in del_table.values():
                    print(v["status"])

                    if v["status"] == 2:
                        eachid["ftp"].append(
                            'cd /p300/gsafileprocess/temp/{}'.format(v["cra_accession"]))
                    elif v["status"] == 3 and v["release_state"] == 2:
                        eachid["ftp"].append(
                            'cd {}/{}'.format(public_root_dict[str(v["public_root"])], v["cra_accession"]))
                    elif v["status"] == 3 and v["release_state"] == 1:
                        # ressss["ftp"].append(
                        #     'cd /p300/gsaFileSite/{}'.format(v["cra_accession"]))
                        eachid["ftp"].append(
                            'cd {}/{}'.format(archive_root_dict[str(v["archive_root"])], v["cra_accession"]))

                    eachid["ftp"].append(
                        'rm -rf {}'.format(' '.join(v["accession"])))
                    if v["status"] != 2:
                        eachid['ftp'].append(
                            'grep -v "{}" md5sum.txt >md5sum_new.txt'.format("\|".join(v["accession"])))
                        eachid["ftp"].append('mv md5sum_new.txt md5sum.txt')
                    eachid["status"] = True
                ress.append(eachid)

            elif eachid["type"] == "CRXacc":
                # print(cra_id_list)
                res1 = gsautilis.del_fetch_exp_by_experiment_acc(cra_id_list)
                # print(res1)
                exp_list = []
                sample_list = []
                # print(res1[1],type(res1[1]))
                for i in res1[1]:
                    exp_list.append(i[0])
                    sample_list.append(i[1])
                expid_lis = ",".join(str(i) for i in exp_list)
                sample_lis = ",".join(str(i) for i in sample_list)
                eachid['mysql'].append(
                    'update experiment set status=5 where exp_id in ({});'.format(expid_lis))
                eachid['mysql'].append(
                    'update run set status=5 where exp_id in ({});'.format(expid_lis))
                eachid['mysql'].append(
                    'update run_data_file set status=5 where exp_id in ({});'.format(expid_lis))

                res2 = gsautilis.del_fetch_sample_by_exp_id(expid_lis)
                if len(res2[1]) != 0:
                    pass
                else:
                    eachid['mysql'].append(
                        'update sample set status=5 where sample_id in ({});'.format(sample_lis))

                res3 = gsautilis.del_fetch_cra_run_by_experiment_id(expid_lis)
                # print(ressss['mysql'])

                del_table = {}
                for i in res3[1]:
                    tempres = {y: z for y, z in i._asdict().items()}
                    print(tempres)
                    if tempres['cra_accession'] not in del_table.keys():
                        del_table[tempres['cra_accession']] = tempres
                        del_table[tempres['cra_accession']]['accession'] = [
                            tempres['accession']]
                    else:
                        del_table[tempres['cra_accession']]['accession'].append(
                            tempres['accession'])
                print(json.dumps(del_table, indent=4))

                for v in del_table.values():
                    print(v["status"])

                    if v["status"] == 2:
                        eachid["ftp"].append(
                            'cd /p300/gsafileprocess/temp/{}'.format(v["cra_accession"]))
                    elif v["status"] == 3 and v["release_state"] == 2:
                        eachid["ftp"].append(
                            'cd {}/{}'.format(public_root_dict[str(v["public_root"])], v["cra_accession"]))
                    elif v["status"] == 3 and v["release_state"] == 1:
                        # ressss["ftp"].append(
                        #     'cd /p300/gsaFileSite/{}'.format(v["cra_accession"]))
                        eachid["ftp"].append(
                            'cd {}/{}'.format(archive_root_dict[str(v["archive_root"])], v["cra_accession"]))

                    # elif v["status"] == 3 and v["release_state"] == 2:
                    #     ressss["ftp"].append(
                    #         'cd /gsapub/ftp/pub/gsa/{}'.format(v["cra_accession"]))
                    # elif v["status"] == 3 and v["release_state"] == 1:
                    #     ressss["ftp"].append(
                    #         'cd /p300/gsaFileSite/{}'.format(v["cra_accession"]))

                    eachid["ftp"].append(
                        'rm -rf {}'.format(' '.join(v["accession"])))
                    if v["status"] != 2:
                        eachid['ftp'].append(
                            'grep -v "{}" md5sum.txt >md5sum_new.txt'.format("\|".join(v["accession"])))
                        eachid["ftp"].append('mv md5sum_new.txt md5sum.txt')
                    eachid["status"] = True
                ress.append(eachid)

            elif eachid["type"] == "CRRacc":
                res1 = gsautilis.del_fetch_run_by_run_acc(cra_id_list)
                run_list = []
                exp_list = []
                sample_list = []
                for i in res1[1]:
                    run_list.append(i[0])
                    exp_list.append(i[1])
                    sample_list.append(i[2])
                run_lis = ",".join(str(i) for i in run_list)
                expid_lis = ",".join(str(i) for i in exp_list)
                sample_lis = ",".join(str(i) for i in sample_list)
                eachid['mysql'].append(
                    'update run set status=5 where run_id in ({});'.format(run_lis))
                eachid['mysql'].append(
                    'update run_data_file set status=5 where run_id in ({});'.format(run_lis))

                res2 = gsautilis.del_fetch_exp_by_run_id(run_lis)
                res3 = gsautilis.del_fetch_sample_by_run_id(run_lis)

                if len(res2[1]) != 0:
                    pass
                else:
                    eachid['mysql'].append(
                        'update experiment set status=5 where exp_id in ({});'.format(expid_lis))

                if len(res3[1]) != 0:
                    pass
                else:
                    eachid['mysql'].append(
                        'update sample set status=5 where sample_id in ({});'.format(sample_lis))

                res4 = gsautilis.del_fetch_cra_run_by_run_id(run_lis)

                del_table = {}
                for i in res4[1]:
                    tempres = {y: z for y, z in i._asdict().items()}
                    if tempres['cra_accession'] not in del_table.keys():
                        del_table[tempres['cra_accession']] = tempres
                        del_table[tempres['cra_accession']]['accession'] = [
                            tempres['accession']]
                    else:
                        del_table[tempres['cra_accession']]['accession'].append(
                            tempres['accession'])

                for v in del_table.values():

                    if v["status"] == 2:
                        eachid["ftp"].append(
                            'cd /p300/gsafileprocess/temp/{}'.format(v["cra_accession"]))
                    elif v["status"] == 3 and v["release_state"] == 2:
                        eachid["ftp"].append(
                            'cd {}/{}'.format(public_root_dict[str(v["public_root"])], v["cra_accession"]))
                    elif v["status"] == 3 and v["release_state"] == 1:

                        eachid["ftp"].append(
                            'cd {}/{}'.format(archive_root_dict[str(v["archive_root"])], v["cra_accession"]))

                    eachid["ftp"].append(
                        'rm -rf {}'.format(' '.join(v["accession"])))
                    if v["status"] != 2:
                        eachid['ftp'].append(
                            'grep -v "{}" md5sum.txt >md5sum_new.txt'.format("\|".join(v["accession"])))
                        eachid["ftp"].append('mv md5sum_new.txt md5sum.txt')
                    eachid["status"] = True
                ress.append(eachid)

        except Exception as e:
            return {"status": "error", "data": str(e)}

    return ress


@app.post("/api/ResetBiosampleStatus/", tags=["minismartV2"])
def ResetbiosampleStatus(idList: List):
    res = []
    for eachid in idList:
        if eachid["type"] == "subSAM":
            id = eachid["value"]
            status = gsautilis2.CheckBiosampleFailStatus(id)
            if "3" in status:
                sql = [f"update sample set status=2 where submissionId=\"{id}\";",
                       f"update sample_submission set status=2 where sample_submissionId=\"{id}\";"]
                eachid["status"] = True
                eachid["mysql"] = sql
            else:
                eachid["status"] = False
            res.append(eachid)
    return res


@app.post("/api/ResetMetaCheckStatus/", tags=["minismartV2"])
def resetmetacheckstatus(idList: List):
    res = []
    for eachid in idList:
        cra_id = eachid["value"].strip().replace("subCRA", "")
        status = gsautilis2.CheckMetaFailStatus(cra_id)
        if "4" in status:
            sql = [f"update experiment set status=2 where cra_id={cra_id};",
                   f"update run_data_file set status=2 where cra_id={cra_id};", f"update run set status=2 where cra_id={cra_id};"]
            eachid["status"] = True
            eachid["mysql"] = sql
        else:
            eachid["status"] = False
        res.append(eachid)
    return res


@app.post("/api/ReleaseCallback/", tags=["minismartV2"])
def releasecallback(idList: List):
    ress = []

    for eachid in idList:
        eachid["mysql"] = []
        eachid["ftp"] = []

        try:
            if eachid["type"] == "CRAacc":
                back_id = eachid["value"].replace("cra", "CRA")
                res = gsautilis.call_back_by_acc(back_id)
                cra_id = res[0]
                prj_id = res[1]
                public_path = res[6]
                archieve_path = res[5]
                sample_submissionID = res[4][0][0]
                new_rel_time = eachid["releasetime"]
                eachid['mysql'].append('update cra set release_state=1,release_time="{}" where cra_id={};'.format(
                    new_rel_time, cra_id))
                eachid['mysql'].append(
                    'update experiment set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
                eachid['mysql'].append(
                    'update run set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
                eachid['mysql'].append(
                    'update run_data_file set release_state=1 where cra_id={};'.format(cra_id))
                if len(res[2]) == 0:
                    eachid['mysql'].append(
                        'update project set release_state=1,release_time=\"{}\" where prj_id={};'.format(new_rel_time, prj_id))
                else:
                    pass
                if len(res[3]) == 0:
                    eachid['mysql'].append('update sample set release_state=1,release_time=\"{}\" where submissionId=\"{}\";'.format(
                        new_rel_time, sample_submissionID))
                    eachid['mysql'].append('update sample_submission set release_state=1,release_time=\"{}\" where sample_submissionId=\"{}\";'.format(
                        new_rel_time, sample_submissionID))
                else:
                    pass
                eachid['ftp'].append(
                    "cp -r {}/{} {}/".format(public_path, back_id, archieve_path))
                eachid['ftp'].append(
                    'rm -r {}/{}'.format(public_path, back_id))
                eachid["status"] = True
                ress.append(eachid)
                # print(eachid)

            elif eachid["type"] == "subCRA":
                back_id = eachid["value"].replace("subCRA0", "")
                res = gsautilis.call_back_by_cra_id(back_id)
                cra_acc = res[0]
                public_path = res[6]
                archieve_path = res[5]
                prj_id = res[1]
                cra_id = back_id
                sample_submissionID = res[4][0][0]
                new_rel_time = eachid["releasetime"]
                eachid['mysql'].append(
                    'update cra set release_state=1,release_time="{}" where cra_id={};'.format(new_rel_time, cra_id))
                eachid['mysql'].append(
                    'update experiment set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
                eachid['mysql'].append(
                    'update run set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
                eachid['mysql'].append(
                    'update run_data_file set release_state=1 where cra_id={};'.format(cra_id))
                if len(res[2]) == 0:
                    eachid['mysql'].append(
                        'update project set release_state=1,release_time=\"{}\" where prj_id={};'.format(new_rel_time, prj_id))
                else:
                    pass
                if len(res[3]) == 0:
                    eachid['mysql'].append('update sample set release_state=1,release_time=\"{}\" where submissionId=\"{}\";'.format(
                        new_rel_time, sample_submissionID))
                    eachid['mysql'].append('update sample_submission set release_state=1,release_time=\"{}\" where sample_submissionId=\"{}\";'.format(
                        new_rel_time, sample_submissionID))
                else:
                    pass

                eachid['ftp'].append(
                    'cp -r {}/{} {}/'.format(public_path, cra_acc, archieve_path))
                eachid['ftp'].append(
                    'rm -rf {}/{}'.format(public_path, back_id))
                eachid["status"] = True
                ress.append(eachid)

        except Exception as e:
            break

    return ress


@app.post('/api/Humanfeedback/', tags=["minismartV2"])  # 用于报错反馈
def Humanfeedback(idList: Dict):

    email = idList["email"]
    res = {"email": email,
           "idlist": []}
    for eachid in idList["idlist"]:
        subHRA = eachid["value"].strip("").replace("subHRA", '')
        print(subHRA)
        temp = autoGSAHumanReport(subHRA, email)
        # print(temp)
        if temp["status"] == "ok":
            eachid["status"] = True
        else:
            eachid["status"] = False
        print(eachid)
        res["idlist"].append(eachid)


    return res


@app.post('/api/feedback/', tags=["minismartV2"])  # 用于报错反馈
def problemfeedbackV2(idList: Dict):

    email = idList["email"]
    res = {"email": email,
           "idlist": []}
    for eachid in idList["idlist"]:
        subCRA = eachid["value"].strip("").replace("subCRA", '')
        temp = autoGSAReport(subCRA, email)
        # print(temp)
        if temp["status"] == "ok":
            eachid["status"] = True
        else:
            eachid["status"] = False
        # print(eachid)
        res["idlist"].append(eachid)

    return res


# 用于id check是否为有效ID以及状态查询
@app.post('/api/CheckIdStatus/', tags=["minismartV2"])
def CheckIdStatus(idList: List):
    print(idList)
    res = []
    for eachid in idList:
        if eachid["type"] == "subCRA":
            temp = gsautilis2.CheckAcc(eachid["type"], eachid["value"])
            if temp["status"] == True:
                eachid["results"] = {"is_valid": True, "name": eachid["value"],
                                     "modtime": temp["data"]["modify_time"].strftime("%Y-%m-%d %H:%M:%S"), "status": temp["data"]["status"]}
                res.append(eachid)
            else:
                eachid["results"] = {
                    "is_valid": False, "name": eachid["value"]}
                res.append(eachid)
        else:
            eachid["results"] = {"is_valid": gsautilis2.CheckAcc(
                eachid["type"], eachid["value"])["status"]}
            res.append(eachid)
    return res


@app.post('/api/CheckStatusCount/', tags=["minismartV2"])  # 用于主页的数目统计
def CheckStatusCount():
    res = gsautilis2.CheckStatusCount()
    return res


@app.post('/api/fail2back/')
def fail2back(cid: int):
    res = gsautilis.fail2back(cid)
    return res


@app.post('/api/sample2back/')
def sample2back(sampleacc: str):
    res = gsautilis.sample2back(sampleacc)
    return res


@app.post('/api/modify_status/')
def modify_status(cid: int, date: str, time: str):
    res = gsautilis.modify_status(cid, date, time)
    return res


@app.post("/api/callback/")
def callback(cdate: str, back_id: str, cratype: str):

    ressss = {
        "mysql": [],
        "ftp": []
    }

    try:
        if cratype == "cra":
            back_id = back_id.strip("\n").replace("cra", "CRA")
            res = gsautilis.call_back_by_acc(back_id)
            cra_id = res[0]
            prj_id = res[1]
            public_path = res[6]
            archieve_path = res[5]
            sample_submissionID = res[4][0][0]
            new_rel_time = cdate + " 00:00:00"
            ressss['mysql'].append(
                'update cra set release_state=1,release_time="{}" where cra_id={};'.format(new_rel_time, cra_id))
            ressss['mysql'].append(
                'update experiment set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
            ressss['mysql'].append(
                'update run set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
            ressss['mysql'].append(
                'update run_data_file set release_state=1 where cra_id={};'.format(cra_id))
            if len(res[2]) == 0:
                ressss['mysql'].append(
                    'update project set release_state=1,release_time=\"{}\" where prj_id={};'.format(new_rel_time, prj_id))
            else:
                pass
            if len(res[3]) == 0:
                ressss['mysql'].append('update sample set release_state=1,release_time=\"{}\" where submissionId=\"{}\";'.format(
                    new_rel_time, sample_submissionID))
                ressss['mysql'].append('update sample_submission set release_state=1,release_time=\"{}\" where sample_submissionId=\"{}\";'.format(
                    new_rel_time, sample_submissionID))
            else:
                pass

            ressss['ftp'].append(
                'cp -r {}/{} {}/'.format(public_path, back_id, archieve_path))
            ressss['ftp'].append('rm -rf {}/{}'.format(public_path, back_id))

        elif cratype == "subcra":
            back_id = back_id.strip("\n")
            res = gsautilis.call_back_by_cra_id(back_id)
            cra_acc = res[0]
            public_path = res[6]
            archieve_path = res[5]
            prj_id = res[1]
            cra_id = back_id
            sample_submissionID = res[4][0][0]
            new_rel_time = cdate + " 00:00:00"
            ressss['mysql'].append(
                'update cra set release_state=1,release_time="{}" where cra_id={};'.format(new_rel_time, cra_id))
            ressss['mysql'].append(
                'update experiment set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
            ressss['mysql'].append(
                'update run set release_state=1,release_time=\"{}\" where cra_id={};'.format(new_rel_time, cra_id))
            ressss['mysql'].append(
                'update run_data_file set release_state=1 where cra_id={};'.format(cra_id))
            if len(res[2]) == 0:
                ressss['mysql'].append(
                    'update project set release_state=1,release_time=\"{}\" where prj_id={};'.format(new_rel_time, prj_id))
            else:
                pass
            if len(res[3]) == 0:
                ressss['mysql'].append('update sample set release_state=1,release_time=\"{}\" where submissionId=\"{}\";'.format(
                    new_rel_time, sample_submissionID))
                ressss['mysql'].append('update sample_submission set release_state=1,release_time=\"{}\" where sample_submissionId=\"{}\";'.format(
                    new_rel_time, sample_submissionID))
            else:
                pass

            ressss['ftp'].append(
                'cp -r {}/{} {}/'.format(public_path, cra_acc, archieve_path))
            ressss['ftp'].append('rm -rf {}/{}'.format(public_path, back_id))

    except Exception as e:
        return {"status": "error", "data": str(e)}

    return {"status": "ok", "data": ressss}


@app.post("/api/unsubmitminderquery/")
def unsubmitted_id(star_tim: str, end_tim: str, status: Optional[str] = None):

    cra_id_Lis = gsautilis.unsubmitted_id(status, star_tim, end_tim)

    return cra_id_Lis


@app.post("/api/unsubmitminder/")
def unsubmitted_id(cra_id: Optional[str] = None):
    print(cra_id)
    cra_id = cra_id.strip(",").split(",")
    print(cra_id)

    for i in cra_id:
        # print(i)
        subcra = gsautilis.makeSubCRA(i)
        emal_mess = gsautilis.auto_reminder(i, subcra)
        msg = emal_mess[1]

        gsautilis.auto_mail(msg, "GSA {}元信息未提交提醒".format(subcra), [
                            "sunyanling@big.ac.cn"], ["sunyanling@big.ac.cn"])
        # print(i)
    return ','.join(cra_id)


@app.post("/api/archievestatuschecking/")
def archieve_status_checking(status_checking: str, cra_id_list: Optional[str] = None):
    print(cra_id_list)
    cra_id = cra_id_list.strip(",").replace("subCRA", "").strip(",")

    ressss = []

    res = gsautilis.archieve_status_checking(cra_id, status_checking)
    # print(res)
    for i in res:
        tem = {}
        tem["checking_cra_id"] = i[0]
        tem['modify_time'] = str(i[1])
        tem['status'] = i[2]
        ressss.append(tem)
    print(ressss)
    return ressss


@app.post("/api/doDelate/")
def doDelate(cratype: str, cra_id_list: Optional[str] = None):
    ressss = {
        "mysql": [],
        "ftp": []
    }
    public_root_dict = {
        "1": "/gsapub/ftp/pub/gsa",
        "2": "/gsapub3/gsa2",
        "3": "/gsapub/gsapub"
    }
    archive_root_dict = {
        "1": "/p300/gsaFileSite",
        "1": "/p300/gsaFileSite",
        "3": "/gsainsdc2/private/gsaFileSite"
    }
    print(cra_id_list)
    cra_id_list = ",".join(
        ['"' + i + '"' for i in cra_id_list.strip().strip(",").split(",")])
    cra_id_list = cra_id_list.replace("subCRA", "")
    print(cra_id_list)

    try:
        if cratype == "cra_acc":
            res1 = gsautilis.del_fetch_cra_id_by_cra_accession(cra_id_list)
            print(res1[1], type(res1[1]))

            prj_temp = []
            cra_temp = []
            sample_submissionID = []

            if not res1[0]:
                print(res1[1])
            else:
                for i in res1[1]:
                    print(str(i[1]), type(str(i[1])))
                    prj_temp.append(str(i[1]))
                    cra_temp.append(str(i[2]))
                prj_id = ",".join(prj_temp)
                cra_id = ",".join(cra_temp)

            res2 = gsautilis.del_fetch_cra_status_by_prj_id(prj_id, cra_id)
            res3 = gsautilis.del_fetch_cra_id_by_sample_status(cra_id)

            res4 = gsautilis.del_fetch_samplesub_id_by_craid(cra_id)

            for i in res4[1]:

                sample_submissionID.append(i[0])
            sample_submissionID1 = ','.join(
                ['"' + i + '"' for i in sample_submissionID])

            ressss['mysql'].append(
                'update cra set status=5 where cra_id in ({});'.format(cra_id))
            ressss['mysql'].append(
                'update experiment set status=5 where cra_id in ({});'.format(cra_id))
            ressss['mysql'].append(
                'update run set status=5 where cra_id in ({});'.format(cra_id))
            ressss['mysql'].append(
                'update run_data_file set status=5 where cra_id in ({});'.format(cra_id))

            if len(res2[1]) == 0:
                ressss['mysql'].append(
                    'update project set status=5 where prj_id in ({});'.format(prj_id))
            else:
                pass

            if len(res3[1]) == 0:
                ressss['mysql'].append(
                    'update sample set status=5 where submissionId in ({});'.format(sample_submissionID1))
                ressss['mysql'].append(
                    'update sample_submission set status=5 where sample_submissionId in ({});'.format(sample_submissionID1))
            else:
                pass

            # #数据删除

            for i in cra_id_list.split(","):

                res5 = gsautilis.del_fetch_cra_id_by_cra_accession(i)
                i = i.strip("\"")
                if res5[1][0][0] == 3 and res5[1][0][3] == 2:
                    ressss["ftp"].append(
                        'rm -rf {}/{}'.format(public_root_dict[str(res5[1][0][5])], i))

                elif res5[1][0][0] == 3 and res5[1][0][3] == 1:
                    ressss["ftp"].append(
                        'rm -rf {}/{}'.format(archive_root_dict[str(res5[1][0][4])], i))

                elif res5[1][0][0] == 4 or res5[1][0][0] == 2:
                    ressss["ftp"].append(
                        'rm -rf /p300/gsafileprocess/temp/{}'.format(i))

            print(type(ressss["ftp"]))

        elif cratype == "subcra":
            res1 = gsautilis.del_fetch_cra_by_cra_id(cra_id_list)

            prj_temp = []
            cra_acc_temp = []
            sample_submissionID = []

            if not res1[0]:
                print(res1[1])
            else:
                for i in res1[1]:
                    print(str(i[1]), type(str(i[1])))
                    prj_temp.append(str(i[1]))
                    cra_acc_temp.append(str(i[2]))

                prj_id = ",".join(prj_temp)
                cra_acc = ",".join(cra_acc_temp)
                print(cra_acc, prj_id)
            res2 = gsautilis.del_fetch_cra_status_by_prj_id(
                prj_id, cra_id_list)
            res3 = gsautilis.del_fetch_cra_id_by_sample_status(cra_id_list)

            res4 = gsautilis.del_fetch_samplesub_id_by_craid(cra_id_list)

            for i in res4[1]:

                sample_submissionID.append(i[0])
            sample_submissionID1 = ','.join(
                ['"' + i + '"' for i in sample_submissionID])
            print(sample_submissionID1)

            ressss['mysql'].append(
                'update cra set status=5 where cra_id in ({});'.format(cra_id_list))
            ressss['mysql'].append(
                'update experiment set status=5 where cra_id in ({});'.format(cra_id_list))
            ressss['mysql'].append(
                'update run set status=5 where cra_id in ({});'.format(cra_id_list))
            ressss['mysql'].append(
                'update run_data_file set status=5 where cra_id in ({});'.format(cra_id_list))

            if len(res2[1]) == 0:
                ressss['mysql'].append(
                    'update project set status=5 where prj_id in ({});'.format(prj_id))
            else:
                pass

            if len(res3[1]) == 0:
                ressss['mysql'].append(
                    'update sample set status=5 where submissionId in ({});'.format(sample_submissionID1))
                ressss['mysql'].append(
                    'update sample_submission set status=5 where sample_submissionId in ({});'.format(sample_submissionID1))
            else:
                pass


            for i in cra_acc.split(","):
                # print(i)

                res5 = gsautilis.del_fetch_cra_id_by_cra_accession(
                    ("\"" + i + "\""))

                if res5[1][0][0] == 3 and res5[1][0][3] == 2:
                    ressss["ftp"].append(
                        'rm -rf {}/{}'.format(public_root_dict[str(res5[1][0][5])], i))

                elif res5[1][0][0] == 3 and res5[1][0][3] == 1:
                    ressss["ftp"].append(
                        'rm -rf {}/{}'.format(archive_root_dict[str(res5[1][0][4])], i))

                elif res5[1][0][0] == 4 or res5[1][0][0] == 2:
                    ressss["ftp"].append(
                        'rm -rf /p300/gsafileprocess/temp/{}'.format(i))

        elif cratype == "sample_acc":
            res1 = gsautilis.del_fetch_sample_by_sample_acc(cra_id_list)

            sample_list = []
            sample_submissionID = []
            for i in res1[1]:
                sample_list.append(i[0])
                sample_submissionID.append("\"" + str(i[1]) + "\"")
            s_lis = ",".join(str(i) for i in sample_list)

            ressss['mysql'].append(
                'update sample set status=5 where sample_id in ({});'.format(s_lis))
            ressss['mysql'].append(
                'update experiment set status=5 where sample_id in ({});'.format(s_lis))
            ressss['mysql'].append(
                'update run set status=5 where sample_id in ({});'.format(s_lis))
            ressss['mysql'].append(
                'update run_data_file set status=5 where sample_id in ({});'.format(s_lis))

            sample_submissionID1 = ",".join(str(i)
                                            for i in set(sample_submissionID))
            for i in sample_submissionID:
                res2 = gsautilis.del_fetch_sample_by_samplesubmissionID(
                    i, s_lis)

                if len(res2[1]) == 0:
                    ressss['mysql'].append(
                        'update sample_submission set status=5 where sample_submissionId in ({});'.format(i))
                else:
                    pass
            res3 = gsautilis.del_fetch_cra_run_by_sample_id(s_lis)
            print(res3)

            del_table = {}
            for i in res3[1]:
                tempres = {y: z for y, z in i._asdict().items()}
                print(tempres)
                if tempres['cra_accession'] not in del_table.keys():
                    del_table[tempres['cra_accession']] = tempres
                    del_table[tempres['cra_accession']]['accession'] = [
                        tempres['accession']]
                else:
                    del_table[tempres['cra_accession']]['accession'].append(
                        tempres['accession'])
            print(json.dumps(del_table, indent=4))

            for v in del_table.values():
                print(v["status"])

                if v["status"] == 2:
                    ressss["ftp"].append(
                        'cd /p300/gsafileprocess/temp/{}'.format(v["cra_accession"]))
                elif v["status"] == 3 and v["release_state"] == 2:
                    ressss["ftp"].append(
                        'cd {}/{}'.format(public_root_dict[str(v["public_root"])], v["cra_accession"]))
                elif v["status"] == 3 and v["release_state"] == 1:
                    ressss["ftp"].append(
                        'cd {}/{}'.format(archive_root_dict[str(v["archive_root"])], v["cra_accession"]))

                ressss["ftp"].append(
                    'rm -rf {}'.format(' '.join(v["accession"])))
                if v["status"] != 2:
                    ressss['ftp'].append(
                        'grep -v "{}" md5sum.txt >md5sum_new.txt'.format("\|".join(v["accession"])))
                    ressss["ftp"].append('mv md5sum_new.txt md5sum.txt')
            print(ressss["ftp"])

        elif cratype == "exp_acc":
            res1 = gsautilis.del_fetch_exp_by_experiment_acc(cra_id_list)
            exp_list = []
            sample_list = []
            for i in res1[1]:
                exp_list.append(i[0])
                sample_list.append(i[1])
            expid_lis = ",".join(str(i) for i in exp_list)
            sample_lis = ",".join(str(i) for i in sample_list)
            ressss['mysql'].append(
                'update experiment set status=5 where exp_id in ({});'.format(expid_lis))
            ressss['mysql'].append(
                'update run set status=5 where exp_id in ({});'.format(expid_lis))
            ressss['mysql'].append(
                'update run_data_file set status=5 where exp_id in ({});'.format(expid_lis))

            res2 = gsautilis.del_fetch_sample_by_exp_id(expid_lis)
            if len(res2[1]) != 0:
                pass
            else:
                ressss['mysql'].append(
                    'update sample set status=5 where sample_id in ({});'.format(sample_lis))

            res3 = gsautilis.del_fetch_cra_run_by_experiment_id(expid_lis)

            del_table = {}
            for i in res3[1]:
                tempres = {y: z for y, z in i._asdict().items()}
                print(tempres)
                if tempres['cra_accession'] not in del_table.keys():
                    del_table[tempres['cra_accession']] = tempres
                    del_table[tempres['cra_accession']]['accession'] = [
                        tempres['accession']]
                else:
                    del_table[tempres['cra_accession']]['accession'].append(
                        tempres['accession'])
            print(json.dumps(del_table, indent=4))

            for v in del_table.values():
                print(v["status"])

                if v["status"] == 2:
                    ressss["ftp"].append(
                        'cd /p300/gsafileprocess/temp/{}'.format(v["cra_accession"]))
                elif v["status"] == 3 and v["release_state"] == 2:
                    ressss["ftp"].append(
                        'cd {}/{}'.format(public_root_dict[str(v["public_root"])], v["cra_accession"]))
                elif v["status"] == 3 and v["release_state"] == 1:
                    ressss["ftp"].append(
                        'cd {}/{}'.format(archive_root_dict[str(v["archive_root"])], v["cra_accession"]))

                ressss["ftp"].append(
                    'rm -rf {}'.format(' '.join(v["accession"])))
                if v["status"] != 2:
                    ressss['ftp'].append(
                        'grep -v "{}" md5sum.txt >md5sum_new.txt'.format("\|".join(v["accession"])))
                    ressss["ftp"].append('mv md5sum_new.txt md5sum.txt')
            print(ressss["ftp"])

        elif cratype == "run_acc":
            res1 = gsautilis.del_fetch_run_by_run_acc(cra_id_list)
            run_list = []
            exp_list = []
            sample_list = []
            # print(res1[1],type(res1[1]))
            for i in res1[1]:
                run_list.append(i[0])
                exp_list.append(i[1])
                sample_list.append(i[2])
            run_lis = ",".join(str(i) for i in run_list)
            expid_lis = ",".join(str(i) for i in exp_list)
            sample_lis = ",".join(str(i) for i in sample_list)
            ressss['mysql'].append(
                'update run set status=5 where run_id in ({});'.format(run_lis))
            ressss['mysql'].append(
                'update run_data_file set status=5 where run_id in ({});'.format(run_lis))

            res2 = gsautilis.del_fetch_exp_by_run_id(run_lis)
            res3 = gsautilis.del_fetch_sample_by_run_id(run_lis)

            if len(res2[1]) != 0:
                pass
            else:
                ressss['mysql'].append(
                    'update experiment set status=5 where exp_id in ({});'.format(expid_lis))

            if len(res3[1]) != 0:
                pass
            else:
                ressss['mysql'].append(
                    'update sample set status=5 where sample_id in ({});'.format(sample_lis))

            res4 = gsautilis.del_fetch_cra_run_by_run_id(run_lis)

            del_table = {}
            for i in res4[1]:
                tempres = {y: z for y, z in i._asdict().items()}
                print(tempres)
                if tempres['cra_accession'] not in del_table.keys():
                    del_table[tempres['cra_accession']] = tempres
                    del_table[tempres['cra_accession']]['accession'] = [
                        tempres['accession']]
                else:
                    del_table[tempres['cra_accession']]['accession'].append(
                        tempres['accession'])
            print(json.dumps(del_table, indent=4))

            for v in del_table.values():
                print(v["status"])

                if v["status"] == 2:
                    ressss["ftp"].append(
                        'cd /p300/gsafileprocess/temp/{}'.format(v["cra_accession"]))
                elif v["status"] == 3 and v["release_state"] == 2:
                    ressss["ftp"].append(
                        'cd {}/{}'.format(public_root_dict[str(v["public_root"])], v["cra_accession"]))
                elif v["status"] == 3 and v["release_state"] == 1:
                    ressss["ftp"].append(
                        'cd {}/{}'.format(archive_root_dict[str(v["archive_root"])], v["cra_accession"]))

                ressss["ftp"].append(
                    'rm -rf {}'.format(' '.join(v["accession"])))
                if v["status"] != 2:
                    ressss['ftp'].append(
                        'grep -v "{}" md5sum.txt >md5sum_new.txt'.format("\|".join(v["accession"])))
                    ressss["ftp"].append('mv md5sum_new.txt md5sum.txt')
            print(ressss["ftp"])

    except Exception as e:
        return {"status": "error", "data": str(e)}

    return ressss


@app.post("/api/problemfeedback/")
def problemfeedback(cra_id_list: Optional[str] = None):

    check_lis = cra_id_list.strip(",").replace("subCRA0", '').split(",")

    res = []
    return_cra_idok = []
    return_cra_idfail = []
    for k in check_lis:

        res.append(autoGSAReport(k))
    for i in res:
        if i["status"] == "ok":
            return_cra_idok.append(i["data"]["craid"])
        else:
            return_cra_idfail.append(i["data"]["craid"])

    return return_cra_idok, return_cra_idfail


@app.post("/api/fetch_journal/")
def fetch_journal():
    res = gsautilis.fetch_journal()
    journal = []
    for i in res[1]:
        journal.append(i[0])
    return ",".join(journal)


@app.post("/api/fetch_journal_publisher/")
def fetch_journal_publisher(jour_lis: str):
    res = map_pub.Sel_Pub(jour_lis)
    return res


@app.post("/api/GSA2XML/")
def GSA2XML(cra_lis: str):
    stats = generate_by_craacc(cra_lis)
    if stats == True:

        filename = '{}_submission.xml'.format(cra_lis)
        join_result = os.path.join("/workspace/project/GSA/GSA_XML/", filename)
        return FileResponse(
            join_result,
            filename=filename
        )

    else:
        return {"status": 1, "data": f"error, generate xml file for {cra_lis} failed"}
