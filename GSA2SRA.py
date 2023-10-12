from Utilis import GSAUtilis2
from UserPackage import UserPackage
from Sqlite3Utilis import sqlite3db
from timeUtilis import currenttime
import os
from GSA2xml_temp import generate_by_craacc
import subprocess
import time
import lxml.etree as ET
# from setFileTransStatus import fileTransfer 

public_root_dict = UserPackage("public_root_dict")
sample_type = UserPackage("sample_type")
archive_path = UserPackage("public_root_dict")
sra = UserPackage("gsa2sra")
gsautilis = GSAUtilis2()
sq = sqlite3db("GSA2SRA.db")




def formCRR(data):  # 将需要插入三个表格的数据整理
    crr = []
    cra = set()
    samc = {}

    for eachcrr in data:
        onecrr = {"CRA_ACC": "", "CRR_ACC": "", "CRX_ACC": "SAMC_ACC",
                  "PRJC_ACC": "", "SAMPLE_type": "", "archive_path": ""}
        onecrr["CRA_ACC"] = eachcrr["CRAacc"]
        onecrr["CRR_ACC"] = eachcrr["CRRacc"]
        onecrr["CRX_ACC"] = eachcrr["CRXacc"]
        onecrr["SAMC_ACC"] = eachcrr["SAMCacc"]
        onecrr["PRJC_ACC"] = eachcrr["PRJCAacc"]
        onecrr["SAMPLE_type"] = sample_type[str(eachcrr["sample_type_id"])]
        onecrr["archive_path"] = archive_path[str(
            eachcrr["public_root"])] + eachcrr["CRAacc"]

        crr.append(onecrr)
        cra.add(eachcrr["CRAacc"])
        samc[eachcrr["SAMCacc"]] = onecrr["PRJC_ACC"]
    return crr, cra, samc


def InsertData2Sqlite():  # 处理扫描数据的需求，判断哪些数据是
    # 如果新增的crr,cra,sample 则对应插入到三个表，否则不做插入；如果有撤回数据，导致不在表格中的，则task表格的cra状态修改为4
    result = {"status": False, "adddata": {
        "CRR": {},
        "TASK": {},
        "LOCK": {}
    }}
    release_data2SRA = gsautilis.ScanData2SRA()
    test = []  #这个是暂时用的，为了防止数据过大，反复插入，所有用test取特定的cra
    for i in release_data2SRA:
        # if i["CRAacc"] == "CRA010500":
        #     test.append(i)
        if i["CRAacc"] == "CRA012786":
            test.append(i)

    crr, cra, samc = formCRR(test)
    cra_in_lock = sq.fetchaccessionInTASK()

    current_time = currenttime()
    newcrr = []
    for eachline in crr:  # crr数据每行判断，如果没有的就插入，如果有的就跳过
        # print(eachline)
        sra_samn=sq.fetchSAMNfromCRR(eachline["SAMC_ACC"])
        sra_prjna=sq.fetchPRJNAfromCRR(eachline["PRJC_ACC"])
        eachcrr=[]
        try:
            if sra_samn["SRA_SAMPLE"] ==None and sra_prjna["SRA_PRJ_ACC"] ==None :
                # print("aaaaaaaaaaaaaa")
                eachcrr.extend([eachline["CRA_ACC"], eachline["CRR_ACC"], eachline["CRX_ACC"],
                        eachline["SAMC_ACC"], eachline["PRJC_ACC"], eachline["SAMPLE_type"], eachline["archive_path"]])
            elif sra_samn["SRA_SAMPLE"] !=None and sra_prjna["SRA_PRJ_ACC"] !=None:
                eachcrr.extend([eachline["CRA_ACC"], eachline["CRR_ACC"], eachline["CRX_ACC"],
                        eachline["SAMC_ACC"], eachline["PRJC_ACC"], eachline["SAMPLE_type"], eachline["archive_path"],sra_prjna["SRA_PRJ_ACC"],sra_prjna["SRA_SAMPLE"]])
            elif sra_samn["SRA_SAMPLE"] ==None and sra_prjna["SRA_PRJ_ACC"] !=None:
                eachcrr.extend([eachline["CRA_ACC"], eachline["CRR_ACC"], eachline["CRX_ACC"],
                        eachline["SAMC_ACC"], eachline["PRJC_ACC"], eachline["SAMPLE_type"], eachline["archive_path"],sra_prjna["SRA_PRJ_ACC"]])
            # elif sra_samn!=None and sra_prjna==None: #应该暂时没有这种情况
            #     eachcrr = [eachline["CRA_ACC"], eachline["CRR_ACC"], eachline["CRX_ACC"],
            #             eachline["SAMC_ACC"], eachline["PRJC_ACC"], eachline["SAMPLE_type"], eachline["archive_path"],sra_prjna["SRA_SAMPLE"]]

        finally:
            res1 = sq.fetchfromCRR("CRR_ACC", str(eachline["CRR_ACC"]))
            if len(res1) == 0:
                sq.innsertCRRtable(eachcrr)
                newcrr.append(eachline["CRR_ACC"])
            else:
                pass
        
    if len(newcrr) != 0:
        result["adddata"]["CRR"] = {"status": True,
                                    "accession": newcrr, "number": len(newcrr)}
    else:
        result["adddata"]["CRR"] = {"status": False,
                                    "accession": "无新增数据", "number": 0}

    cra = list(cra)
    # 计算在lock表中有的cra，但是在最新的cra表格中没有，后续将通过修改状态为4的方式，将其暂时锁定
    extra_item_in_lock = [item for item in cra_in_lock if item not in cra]
    print(extra_item_in_lock)
    newcra = []
    lock_failed=[]
    for eachcra in cra:
        print(eachcra)
        # 查询数据是否在表格中，如果不在，则插入，如果在，但是之前锁定了，就重新再打开
        res = sq.fetchfromTASK(str(eachcra))
        # print(res)
        if len(res) == 0:
            cra_acc = [str(eachcra), 0, 0, 0, 0, "", "", 0, current_time, ""]
            sq.innserTASKtable(cra_acc)
            newcra.append(str(eachcra))
        else:
            if res[0]["SUBMIT_STATUS"] == "4":
                # 修改状态为0
                sq.updateTASKtable("SUBMIT_STATUS", 0, str(eachcra))
            else:
                pass

    if len(newcra) != 0:
        result["adddata"]["TASK"] = {"status": True,
                                     "accession": newcra, "number": len(newcra)}
    else:
        result["adddata"]["TASK"] = {"status": False,
                                     "accession": "无新增数据", "number": 0}

    if len(extra_item_in_lock) != 0:
        result["adddata"]["lockdata"] = {
            "status": True, "accession": extra_item_in_lock, "number": len(extra_item_in_lock)}
        for eachcra in extra_item_in_lock:
            res = sq.fetchfromTASK(str(eachcra))
            if res[0]["SUBMIT_STATUS"] ==1 or res[0]["SUBMIT_STATUS"]==2:
                lock_failed.append(str(eachcra))
                pass
            else:
                sq.updateTASKtable("SUBMIT_STATUS", 4, str(eachcra))
    else:
        result["adddata"]["lockdata"] = {
            "status": False, "accession": "无新增数据", "number": 0}

    newsamc = []
    for k, v in samc.items():
        res = sq.fetchfromLOCK("SAMC_ACC", str(k))
        print(res)
        if res == None:
            samcacc = [str(k), str(v), 0, 0]
            sq.insertLOCKtable(samcacc)
            newsamc.append(str(k))
        else:
            pass
    if len(newsamc) != 0:
        result["adddata"]["LOCK"] = {"status": True,
                                     "accession": newsamc, "number": len(newsamc)}
    else:
        result["adddata"]["LOCK"] = {"status": False,
                                     "accession": "无新增数据", "number": 0}
        
    if len(lock_failed)!=0:
        result["adddata"]["LOCKFailed"] = {"status": True,
                                     "accession": lock_failed, "number": len(lock_failed)}
    else:
        result["adddata"]["LOCKFailed"] = {"status": False,
                                     "accession": "无新增数据", "number": 0}

    if result["adddata"]["LOCK"]["number"] != 0 or result["adddata"]["CRR"]["number"] != 0 or result["adddata"]["LOCK"]["number"] != 0 or result["adddata"]["CRR"]["number"] != 0 or result["adddata"]["LOCKFailed"]["number"]:
        result["status"] = True
    return result

def GenerateGSA2SRApath(crapath):
    try:
        if os.path.exists(crapath):
            pass
        else:
            os.mkdir(crapath)
            return {"status":True,"msg":"创建文件夹成功:"+ str(crapath)}
    except Exception as e:
       return {"status":False,"msg":str(e)}

def XMLgenerate(CRAacc):
    generate_by_craacc(CRAacc)
    xml=generateTaskPath(CRAacc)+"/submission.xml"
    if os.path.exists(xml):
        return {"status":True,"msg":"submission.xml generate succeed"}
    else:
        return {"status":False,"msg":"submission.xml generate failed"}

def reportMd5(report):
    result = subprocess.run(["md5sum", report], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output = result.stdout
    md5_value = output.split()[0]
    return md5_value

def is_PRJ_SAMC_acc_lock(CRAacc):
    res = sq.fetchPrjSamcAccandStatusByCRAacc(CRAacc)
    status = True
    for i in res:
        if i["IS_PRJC_LOCK"] != 0 or i["IS_SAMC_LOCK"] != 0:  # 只有在未分配编号并且首次提交的时候 才需要锁。
            status = False
            break
        else:
            pass
    return status

def Prj_SAMC_LOCKacc(CRAacc):
    lock_status=sq.fetchPrjSamcAccandStatusByCRAacc(CRAacc)
    lock_samc=[]
    lock_prj=""
    for i in lock_status:
        if i["IS_SAMC_LOCK"]==1:
            lock_samc.append(i["SAMC_ACC"])
        else:
            pass
        if i["IS_PRJC_LOCK"]==1:
            lock_prj=i["PRJC_ACC"]
        else:
            pass
    return lock_prj,lock_samc

def PrepareReady(CRAacc):
    task_path = sq.fetchfromTASK(CRAacc)[0]["TASK_PATH"]
    file=task_path +"/submit.ready"
    if os.path.isfile(file) ==False:
        with open(file, 'w') as file:
            pass
    else:
        pass

def TASKstatus(CRAacc):
    result={}
    allstatus=sq.fetchfromTASK(CRAacc)[0]
    result["XML_STATUS"]=allstatus["XML_STATUS"]
    result["FQ_STATUS"]=allstatus["FQ_STATUS"]
    result["UPLOAD_XML"]=allstatus["UPLOAD_XML"]
    result["SUBMIT_STATUS"]=allstatus["SUBMIT_STATUS"]
    result["REPORT_XML_STATUS"]=allstatus["REPORT_XML_STATUS"]
    result["XML_STATUS"]=allstatus["XML_STATUS"]
    return result

def DetectFqTransfer(CRAacc):
    nohup_command=f'bash {"/workspace/project/GSA/GSA2SRA/" +CRAacc +"/ascpsubmitfq.sh"}' 
    command = f'ps aux |grep -w "{nohup_command}" |grep -v grep'
    popen =subprocess.Popen(command,shell=True,stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out,err = popen.communicate()
    if out: #不为空，表示程序正在运行
        return {"status":True,"msg":"程序正在运行"}
    elif err:
        return {"status":False,"msg":str}
    else:
        return {"status":True,"msg":"程序运行完成"}

def CRAinTASKstatistic():
    res=sq.fetchallsubmissionInTASK()
    result={"all":{"CRA_ACC":[],"number":""},
            "unsubmit":{"CRA_ACC":[],"number":""},
            "submitting":{"CRA_ACC":[],"number":""},
            "succeed":{"CRA_ACC":[],"number":""},
            "failed":{"CRA_ACC":[],"number":""},
            "unablesubmit":{"CRA_ACC":[],"number":""}}
    for i in res:
        result["all"]["CRA_ACC"].append(i["CRA_ACC"])
        if i["SUBMIT_STATUS"]==0:
            result["unsubmit"]["CRA_ACC"].append(i["CRA_ACC"])
        elif i["SUBMIT_STATUS"]==1:
            result["submitting"]["CRA_ACC"].append(i["CRA_ACC"])
        elif i["SUBMIT_STATUS"]==2:
            result["succeed"]["CRA_ACC"].append(i["CRA_ACC"])
        elif i["SUBMIT_STATUS"]==3:
            result["failed"]["CRA_ACC"].append(i["CRA_ACC"])
        elif i["SUBMIT_STATUS"]==4:
            result["unablesubmit"]["CRA_ACC"].append(i["CRA_ACC"])
    
    result["all"]["number"]=len(result["all"]["CRA_ACC"])
    result["unsubmit"]["number"]=len(result["unsubmit"]["CRA_ACC"])
    result["submitting"]["number"]=len(result["submitting"]["CRA_ACC"])
    result["succeed"]["number"]=len(result["succeed"]["CRA_ACC"])
    result["failed"]["number"]=len(result["failed"]["CRA_ACC"])
    result["unablesubmit"]["number"]=len(result["unablesubmit"]["CRA_ACC"])
    return result

def ParaseReportXML(CRAacc):
    result = {
        "submission":{},
        "project":{},
        "sample":[],
        "CRR":[],
        "SRA":""
    }
    task_path = sq.fetchfromTASK(CRAacc)[0]["TASK_PATH"]
    # print(sq.fetchfromTASK(CRAacc))
    report_xml=task_path +"/report.xml"

    et=ET.iterparse(report_xml)
    for event,elem in et:  #event是end，elem是action,
        if event=="end" and elem.tag=="SubmissionStatus":
            status=elem.get("status")
            if status=="failed":
                Message=elem.find("Message").get("severity") +":" + elem.find("Message").text
                result["submission"]=Message
            else:
                submissionid = elem.get("submission_id")
                result["submission"]=submissionid

        elif event =="end" and elem.tag =="Action":
            target_db = elem.get("target_db")
            status=elem.get("status")
            action_id=elem.get("action_id").split("-")[1].split("_")[0].upper()
            message=elem.find("Response").find("Message")
            obje = elem.find("Response").find("Object")
     
        
            if target_db == "bioproject": #elem.keys()可以看这个elem下所有的
                project_info={}
                if status=="processed-ok":  
                    if obje is  not None:
                        accession = obje.get("accession")
                        project_info["PRJCAacc"]=action_id
                        project_info["PRJNAacc"]=accession
                        # project_info[action_id]=accession
                        project_info["status"]=status
                        result["project"]=project_info
                    else:
                        pass
                elif status=="processed-error":
                    error_text=message.text
                    project_info["PRJCAacc"]=action_id
                    project_info["status"]=status
                    project_info["error_txt"]=error_text
                    result["project"]=project_info
                else:
                    pass
            elif target_db == "BioSample":
                sample_info={}
                # sample_info[action_id]={}
                if status=="processed-error":
                    if message is not None:
                        sample_info["SAMCacc"]=action_id
                        sample_info["status"]=status
                        sample_info["error_code"]=message.get("error_code")
                        sample_info["error_txt"]=message.text
                        result["sample"].append(sample_info)

                elif status=="processed-ok":
                    if message is not None:
                        if message.text=="Successfully loaded":
                            if obje is  not None:
                                sample_acc=obje.get("accession")
                                sample_info["SAMCacc"]=action_id
                                sample_info["status"]=status
                                sample_info["SAMNacc"]=sample_acc

                                result["sample"].append(sample_info)
                            else:
                                pass
                        else:
                            pass
                    else:
                        pass
            elif target_db =="SRA":
                run={}
                # run[action_id]={}
                if status=="processing":
                    run["CRRacc"]=action_id
                    run["status"]=status
                    result["CRR"].append(run)
                elif status=="processed-ok":
                        meta=elem.find("Response").find("Object").find("Meta")
                        srastudy=meta.find("SRAStudy").text
                        experimnt=meta.find("Experiment").text

                        run_acc=obje.get("accession")
                        run["CRRacc"]=action_id
                        run["status"]=status
                        run["SRXacc"]=experimnt
                        run["SRRacc"]=run_acc
                        result["CRR"].append(run)
                        result["SRA"]=srastudy
                        # print(srastudy,experimnt
                elif status=="processed-error":
                    error_code=message.get("error_code")
                    run["CRRacc"]=action_id
                    run["status"]=status
                    run["error_code"]=error_code
                    result["CRR"].append(run)

    return result

def ReportXMLStatusCheck(CRAacc):
    result=ParaseReportXML(CRAacc)
    sample_status=[]
    crr_status=[]
    for eachsample in result["sample"]:
        sample_status.append(eachsample["status"])
    for eachcrr in result["CRR"]:
        crr_status.append(eachcrr["status"])

    print(result["submission"])

    if not result["submission"].startswith("SUB"):
        return {"status":False}

    if result["project"]["status"]!="processed-ok" or "processed-error" in sample_status:
        return {"status":False}
    elif result["project"]["status"]=="processed-ok" and  "processed-error" not in sample_status:
        if "processing" in crr_status:
            return {"status":"processing"}
        elif "processed-error" in crr_status:
            return {"status":False}
        else:
            return {"status":True}

def generateTaskPath(CRAacc):
    crapath = "/workspace/project/GSA/GSA2SRA/" + str(CRAacc)
    return crapath

def PRJaccUpdate(CRAacc):
    prj_result=ParaseReportXML(CRAacc)["project"]
    sq.updatePRJinCRR(prj_result["PRJNAacc"],prj_result["PRJCAacc"])
    project_status=sq.fetchfromLOCK("PRJC_ACC",prj_result["PRJCAacc"])["IS_PRJC_LOCK"]
    if project_status==1:
        sq.updateLOCKstatus("PRJC_ACC",prj_result["PRJCAacc"],"IS_PRJC_LOCK",0) #将lock表中的project对应的解锁

def SAMCaccUpdate(CRAacc):
    samc_result=ParaseReportXML(CRAacc)["sample"]
    for eachsample in samc_result:
        sq.updateSAMPLEinCRR(eachsample["SAMNacc"],eachsample["SAMCacc"])
        sample_status=sq.fetchfromLOCK("SAMC_ACC",eachsample["SAMCacc"])["IS_SAMC_LOCK"]
        if sample_status==1:
            sq.updateLOCKstatus("SAMC_ACC",eachsample["SAMCacc"],"IS_SAMC_LOCK",0) #将lock表中的project对应的解锁

def CRRaccUpdate(CRAacc):
    crr_result=ParaseReportXML(CRAacc)["CRR"]
    submission=ParaseReportXML(CRAacc)["submission"]
    sra=ParaseReportXML(CRAacc)["SRA"]
    for eachCRR in crr_result:
        sq.updateCRAincrr(eachCRR["SRXacc"],eachCRR["SRRacc"],sra,submission,eachCRR["CRRacc"])

def step1XMLprepare(CRAacc):
    current_time = currenttime()
    status = is_PRJ_SAMC_acc_lock(CRAacc) #判断cra中的sample和project是否有lock的，如果有，则不可以提交
    if status == True:
        crapath = generateTaskPath(CRAacc)
        generate_path_status=GenerateGSA2SRApath(crapath)["status"] #判断生成task路径的状态
        if generate_path_status==True:
            sq.updateTASKtable("TASK_PATH", crapath, CRAacc)  # 更新task路径
            sq.updateTASKtable("SUBMIT_STATUS", 1, CRAacc) # 路径准备完成后，将submitstatus修改为1
            submission_xml_status=XMLgenerate(CRAacc)["status"]  # 在task路径下生成下xml
            if submission_xml_status==True: 
                sq.updateTASKtable("XML_STATUS", 1, CRAacc)  # 将xml——status的状态修改为1
                sq.updateTASKtable("MODIFY_TIME", current_time, CRAacc) #将modify_time的时间修改
                res = sq.fetchPrjSamcAccandStatusByCRAacc(CRAacc)
                prj = {"PRJC_ACC": "", "SRA_PRJ_ACC": "", "IS_PRJC_LOCK": ""}
                samc = []
                for i in res:
                    prj["PRJC_ACC"] = i["PRJC_ACC"]
                    prj["SRA_PRJ_ACC"] = i["SRA_PRJ_ACC"]
                    prj["IS_PRJC_LOCK"] = i["IS_PRJC_LOCK"]
                    if i["SRA_SAMPLE"] == None and i["IS_SAMC_LOCK"] == 0:
                        samc.append(i["SAMC_ACC"])
                        sq.updateLOCKstatus(
                            "SAMC_ACC", i["SAMC_ACC"], "IS_SAMC_LOCK", 1)
                if prj["SRA_PRJ_ACC"] == None and prj["IS_PRJC_LOCK"] == 0:
                    sq.updateLOCKstatus("PRJC_ACC", prj["PRJC_ACC"], "IS_PRJC_LOCK", 1)

                return {"status":True,"msg":"XML已生成，相关id已lock"}
            else:
                return {"status":False,"msg":"submission.xml generate failed"}   
        else:
            return {"status":False,"msg":"Task Path generate failed"}
    else:
        return {"status": False,"msg":CRAacc+"涉及的project或sample处于lock的状态"}

def step2fqtransfer(CRAacc):
    cra_path = sq.fetchfromCRR("CRA_ACC", CRAacc)[0]["archive_path"]
    task_path = sq.fetchfromTASK(CRAacc)[0]["TASK_PATH"]
    xml_status = sq.fetchfromTASK(CRAacc)[0]["SUBMIT_STATUS"]
    fq_status = sq.fetchfromTASK(CRAacc)[0]["FQ_STATUS"]
    current_time = currenttime()
    if xml_status != 0 and fq_status==2:
        sq.updateTASKtable("FQ_STATUS",1,CRAacc)
        command = f'ascp -i {sra["key"]} -QT -l300m -k1 -d  $(ls {cra_path}/*/*{{.gz,.bam}} 2>/dev/null) {sra["remotepath"]}/{CRAacc}  &> {task_path}/file_transfer.log && python3 /workspace/project/GSA/setFileTransStatus.py {CRAacc} GOOD || python3 /workspace/project/GSA/setFileTransStatus.py {CRAacc} BAD'
        with open(task_path+"/ascpsubmitfq.sh","w") as f:
            f.write(command)
        nohup_command=f'nohup bash {task_path+"/ascpsubmitfq.sh"} &' 
        subprocess.Popen(nohup_command, shell=True)
        sq.updateTASKtable("MODIFY_TIME", current_time, CRAacc)
    else:
        return False
    #     return {"status": True,"msg":"fq文件传输完成"}
    # else:
    #     return {"status": False,"msg":"fq文件无需传输"}
    
def step3submissionSubmit(CRAacc):
    xml_status = sq.fetchfromTASK(CRAacc)[0]["SUBMIT_STATUS"]
    fq_status = sq.fetchfromTASK(CRAacc)[0]["FQ_STATUS"]
    task_path = sq.fetchfromTASK(CRAacc)[0]["TASK_PATH"]
    current_time = currenttime()
    if xml_status==1 and fq_status==2:
        PrepareReady(CRAacc)
        command = f'ascp -i {sra["key"]} -QT -l300m -k1 -d  {task_path}/submi*  {sra["remotepath"]}/{CRAacc}'
        subprocess.Popen(command, shell=True)
        sq.updateTASKtable("UPLOAD_XML",1,CRAacc)
        sq.updateTASKtable("MODIFY_TIME", current_time, CRAacc)
        return {"status": True,"msg":"submission.xml和submit.ready文件传输完成"}
    else:
        return {"status": False,"msg":"submission.xml和submit.ready文件无需传输"}

def step4DownloadParseReport(CRAacc):
    task_path = sq.fetchfromTASK(CRAacc)[0]["TASK_PATH"]
    report_xml=task_path +"/report.xml"
    current_time = currenttime()
    max_try=10
    while True:
        if max_try <=0:
            break
        else:
            upload_xml_status = sq.fetchfromTASK(CRAacc)[0]["UPLOAD_XML"]
            submit_status=sq.fetchfromTASK(CRAacc)[0]["SUBMIT_STATUS"]  
            report_md5=sq.fetchfromTASK(CRAacc)[0]["REPORT_MD5"]
            report_xml_status=sq.fetchfromTASK(CRAacc)[0]["REPORT_XML_STATUS"]
            
            if upload_xml_status==1 and report_xml_status!=2: #就是submission.xml已经提交过去了，但是report还没有成功的状态
                downcommand=f'ascp -i {sra["key"]} -QT -l300m -k1 {sra["remotepath"]}/{CRAacc}/report.xml {task_path}/'
                subprocess.Popen(downcommand, shell=True)
                if os.path.isfile(report_xml)==True:
                    res=ReportXMLStatusCheck(CRAacc)
                    md5=reportMd5(report_xml)
                    if res["status"]==True: #CRR等都处理成功的情况，将CRR插入编号整理好。
                        sq.updateTASKtable("REPORT_MD5",md5,CRAacc)
                        PRJaccUpdate(CRAacc) #在更新project编号的时候一起把lock表中的project解锁
                        SAMCaccUpdate(CRAacc) #在更新sample编号的时候一起把lock表中的sample解锁
                        CRRaccUpdate(CRAacc)
                        sq.updateTASKtable("REPORT_XML_STATUS",2,CRAacc)
                        sq.updateTASKtable("SUBMIT_STATUS",2,CRAacc)
                        sq.updateTASKtable("MODIFY_TIME", current_time, CRAacc)
                        max_try=0
                        return {"status":True,"msg":"数据处理成功"}
                    elif res["status"]==False: #sample或project处理失败了。
                        sq.updateTASKtable("REPORT_MD5",md5,CRAacc)
                        sq.updateTASKtable("REPORT_XML_STATUS",3,CRAacc)
                        sq.updateTASKtable("SUBMIT_STATUS",3,CRAacc)
                        sq.updateTASKtable("MODIFY_TIME", current_time, CRAacc)
                        max_try=0
                        with open(report_xml) as file:
                            file_content=file.read()
                        return {"status":False,"msg":file_content}
                    else:
                        if md5!=report_md5:
                            sq.updateTASKtable("REPORT_MD5",md5,CRAacc)
                            sq.updateTASKtable("MODIFY_TIME", current_time, CRAacc)
                            time.sleep(6)
                            max_try-=1
                        else:
                            time.sleep(6)
                else:
                    time.sleep(6)
                    max_try-=1
            elif upload_xml_status==0:
                return {"status":False,"msg":"submission.xml还未提交"}
            elif submit_status==2:
                return {"status":True,"msg":"数据已提交成功"}

def getStatusByCRAacc(craacc):
    result={"is_LOCKED":{"project":{"status":False,"data":""},"sample":{"status":False,"data":""}},
                 "CRA_ACC":"",
                 "XML_STATUS":"",
                 "FQ_STATUS":"",
                 "UPLOAD_XML":"",
                 "SUBMIT_STATUS":"",
                 "REPORT_MD5":"",
                 "TASK_PATH":"",
                 "REPORT_XML_STATUS":"",
                "SUBMIT_TIME":"",
                 "MODIFY_TIME":""}
    res=sq.fetchfromTASK(craacc)[0]
    lock_prj,lock_samc=Prj_SAMC_LOCKacc(craacc)
    if lock_prj!="":
        result["is_LOCKED"]["project"]["status"]=True
        result["is_LOCKED"]["project"]["data"]=lock_prj
    if len(lock_samc)!=0:
        result["is_LOCKED"]["sample"]["status"]=True
        result["is_LOCKED"]["sample"]["data"]=lock_samc
    result["CRA_ACC"]=res["CRA_ACC"]
    result["XML_STATUS"]=res["XML_STATUS"]
    result["FQ_STATUS"]=res["FQ_STATUS"]
    result["UPLOAD_XML"]=res["UPLOAD_XML"]
    result["SUBMIT_STATUS"]=res["SUBMIT_STATUS"]
    result["REPORT_MD5"]=res["REPORT_MD5"]
    result["TASK_PATH"]=res["TASK_PATH"]
    result["REPORT_XML_STATUS"]=res["REPORT_XML_STATUS"]
    result["SUBMIT_TIME"]=res["SUBMIT_TIME"]
    result["MODIFY_TIME"]=res["MODIFY_TIME"]
    return result

def getStatusByPageNumber(page_number,page_size,sort_value,sort_type,filter_item,filter_value):
    res=sq.fetchfromTASKbyPageSort(page_number,page_size,sort_value,sort_type,filter_item,filter_value)
    result=[]
    for i in res:
        eachcra={"is_LOCKED":{"project":{"status":False,"data":""},"sample":{"status":False,"data":""}},
                 "CRA_ACC":"",
                 "XML_STATUS":"",
                 "FQ_STATUS":"",
                 "UPLOAD_XML":"",
                 "SUBMIT_STATUS":"",
                 "REPORT_MD5":"",
                 "TASK_PATH":"",
                 "REPORT_XML_STATUS":"",
                "SUBMIT_TIME":"",
                 "MODIFY_TIME":""}

        lock_prj,lock_samc=Prj_SAMC_LOCKacc(i["CRA_ACC"])
        if lock_prj!="":
            eachcra["is_LOCKED"]["project"]["status"]=True
            eachcra["is_LOCKED"]["project"]["data"]=lock_prj
        if len(lock_samc)!=0:
            eachcra["is_LOCKED"]["sample"]["status"]=True
            eachcra["is_LOCKED"]["sample"]["data"]=lock_samc
        eachcra["CRA_ACC"]=i["CRA_ACC"]
        eachcra["XML_STATUS"]=i["XML_STATUS"]
        eachcra["FQ_STATUS"]=i["FQ_STATUS"]
        eachcra["UPLOAD_XML"]=i["UPLOAD_XML"]
        eachcra["SUBMIT_STATUS"]=i["SUBMIT_STATUS"]
        eachcra["REPORT_MD5"]=i["REPORT_MD5"]
        eachcra["TASK_PATH"]=i["TASK_PATH"]
        eachcra["REPORT_XML_STATUS"]=i["REPORT_XML_STATUS"]
        eachcra["SUBMIT_TIME"]=i["SUBMIT_TIME"]
        eachcra["MODIFY_TIME"]=i["MODIFY_TIME"]
        result.append(eachcra)
    return result



if __name__ == "__main__":
    # a=InsertData2Sqlite() #step1
    # a=TASKstatus("CRA012728")
    # a = currenttime()

    # # GenerateGSA2SRApath("CRA00000112")
    # # a = step1XMLprepare("CRA000018")
    # print(a)
    # step1XMLprepare("CRA012787")  # step2
    # step2fqtransfer("CRA012787")  # step3
    # step2fqtransfer("CRA012787")

    # ReportXMLStatusCheck("CRA012787")
    # PRJaccUpdate("CRA012787")
    # SAMCaccUpdate("CRA012787")
    # CRRaccUpdate("CRA007333")

    # step3submissionSubmit("CRA012787")
    # step4DownloadParseReport("CRA012787")
    # res111=ParaseReportXML("CRA012787")
    # print(res111)
    # generate_by_craacc("CRA000015")

    # XMLgenerate("CRA000018", "/workspace/project/GSA/GSA2SRA/CRA000018")
    # pass
    # release_data2SRA = gsautilis.ScanData2SRA()
    # test = []
    # for i in release_data2SRA:
    #     if i["CRAacc"] == "CRA012826":
    #         test.append(i)

    # # test = release_data2SRA[-6:]
    # print(test)
    # idList={'page_number': 1, 'page_size': 2, 'sort_value': '', 'sort_type': ''}
    # page_number=idList["page_number"]
    # page_size=idList["page_size"]
    # sort_value=idList["sort_value"]
    # sort_type=idList["sort_type"]
    # res2=getStatusByCRAacc(page_number,page_size,sort_value,sort_type)
    # print(res2)
    # eachline={'CRA_ACC': 'CRA010500', 'CRR_ACC': 'CRR727428', 'CRX_ACC': 'CRX652172', 'PRJC_ACC': 'PRJCA015978', 'SAMPLE_type': 'Model organism or animal sample', 'archive_path': '/gsapub3/gsa2/CRA010500', 'SAMC_ACC': 'SAMC1187520'}
    # sra_samn=sq.fetchSAMNfromCRR(eachline["SAMC_ACC"])
    # sra_prjna=sq.fetchPRJNAfromCRR(eachline["PRJC_ACC"])
    # print(sra_samn["SRA_SAMPLE"])
    # print(sra_prjna["SRA_PRJ_ACC"])
    # a=ParaseReportXML("CRA010500")
    # a=ParaseReportXML("CRA012787")
    # a=ReportXMLStatusCheck("CRA010500")
    a=step4DownloadParseReport("CRA010500")
    print(a)
