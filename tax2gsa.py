import argparse
import os
import sys
import json
import sqlite3
import requests
import prettytable as pt
import pymysql
from dataclasses import dataclass
from typing import List, Dict, Optional
from UserPackage import UserPackage

gsaloginfo = UserPackage("gsadb")

class logging:
    msg = []
    errs = []  # type: ignore

    @classmethod
    def info(cls, msg):
        cls.msg.append(f"INFO:{msg}")

    @classmethod
    def err(cls, msg, type):

        cls.errs.append([type, msg])

    @classmethod
    def get_msgs(cls):
        return "\n".join(cls.msg)

    @classmethod
    def get_error_table(cls):
        tb = pt.PrettyTable()
        tb.align = "l"
        out_table_header = ["Error_Type", "Message"]

        tb.field_names = out_table_header

        tb.add_rows(cls.errs)
        print(tb)
        return tb.get_string()

    @classmethod
    def has_error(cls):
        return len(cls.errs) > 0  # type: ignore


class db():

    chloroplastTree = {
        "2787854": {
            "name": "other entries",
            "gcode": 11
        },
        "2787823": {
            "name": "unclassified entries",
            "gcode": 11
        },
        "2759": {
            "name": "Eukaryota",
            "gcode": 11
        },
        "2570543": {
            "name": "Piridium",
            "gcode": 4
        },
        "2720216": {
            "name": "Apicomplexa sp. corallicolid ex Leiopathes glaberrima",
            "gcode": 4
        },
        "2304055": {
            "name": "Apicomplexa sp. WK-2018_Corallicola",
            "gcode": 4
        },
        "5796": {
            "name": "Coccidia",
            "gcode": 4
        },
        "110120": {
            "name": "Hepatozoon canis",
            "gcode": 11
        },
        "2544991": {
            "name": "Nephromyces sp. ex Molgula occidentalis",
            "gcode": 4
        },
        "462227": {
            "name": "Babesia sp. Xinjiang",
            "gcode": 4
        },
        "462223": {
            "name": "Babesia sp. Lintan",
            "gcode": 4
        },
        "505693": {
            "name": "Chromera velia",
            "gcode": 4
        },
        "2682054": {
            "name": "Volvocales sp. NrCl902",
            "gcode": 4
        },
        "2768738": {
            "name": "Spartinivicinus",
            "gcode": 4
        },
    }

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)
        self.conn.row_factory = db.dict_factory
        self.cur = self.conn.cursor()

    def execute(self, sql, params=None):
        try:
            if params is None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, params)
        except Exception as e:
            self.conn.rollback()
            logging.info(f"Execute sql {sql} enconters error:{e}")

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def fetchOneOrganismByName(self, organism):
        sql = "select * from tax_gcode where name_txt = ? "
        self.execute(sql, (organism, ))
        res = self.fetchone()
        return res

    def fetchOneOrganismByTaxid(self, taxid):
        sql = "select * from tax_gcode where taxon_id = ? "
        self.execute(sql, (taxid, ))
        res = self.fetchone()
        return res

    def findChloroplastgcodeByOrganism(self, organism):
        """
        通过提供物种判断物种的chloroplast的genetic code.

        默认是11. 会根据物种不断查找parent taxon id, 根据ChloroplastTree
        这个对象中记录的判断是否是特殊的 gcode，如果是。则赋值。
        """
        logging.info(f"identifying chloroplast genetic code for {organism}. ")
        gcode = 11
        flag = True
        thisOrganism = self.fetchOneOrganismByName(organism)
        if thisOrganism is None:
            """本地Taxonomy库中找不到物种，让用户联系GenBase管理员"""
            logging.err(
                f"GenBase can not determine the chloroplast genetic code of the organism '{organism}' you provided, please contact curator (genbase@big.ac.cn).",
                "genetic code")
            flag = False
        else:
            taxid = thisOrganism.get("taxon_id")
            iter = 1
            max_iter = 40  # 最大的iteration的数量，超过则跳出循环
            if str(taxid) in self.chloroplastTree.keys(
            ):  # 如果物种的taxid直接就是能在chloroplast tree中能检索到，那么
                gcode = self.chloroplastTree[str(taxid).strip()].get("gcode")
            else:
                # print("Iter fetch parent: ",end="")
                while taxid != 2787854 and taxid != 2787823 and taxid != 2759 and (
                        iter < max_iter):
                    # print(f"{str(iter)}",end=", ")
                    iter += 1
                    record = self.fetchOneOrganismByTaxid(taxid=taxid)
                    if record is None:
                        logging.err(
                            f"GenBase can not determine the chloroplast genetic code of the organism '{organism}' you provided, please contact curator (genbase@big.ac.cn).",
                            "genetic code")
                        flag = False
                        break
                    else:
                        parent_taxon_id = str(record["parent_taxon_id"])
                        if parent_taxon_id in self.chloroplastTree.keys(
                        ):  # 如果物种的上一级能检索到，则直接指定gcode
                            gcode = self.chloroplastTree[parent_taxon_id].get(
                                "gcode")
                            break
                        else:
                            taxid = parent_taxon_id
                logging.info(f"finally found {record}")
        return (flag, gcode)


def clean_taxname(n):
    return n.strip().lower().replace(" ", "")


class GSAUtils():

    def __init__(self):

        self.host = gsaloginfo["username"]
        self.port = gsaloginfo["port"]
        self.username = gsaloginfo["username"]
        self.password = gsaloginfo["password"]
        self.dbname = gsaloginfo["dbname"]
        self.charsets = "UTF8"

        try:
            self.con = pymysql.Connect(host=self.host,
                                       port=int(self.port),
                                       user=self.username,
                                       passwd=self.password,
                                       db=self.dbname,
                                       charset=self.charsets)
            # 获得数据库的游标
            self.cursor = self.con.cursor(
                cursor=pymysql.cursors.DictCursor)  # 开启事务
            logging.info("Get cursor successfully")
        except Exception as e:
            logging.info("Can not connect databse {}\nReason:{}".format(
                self.dbname, e))

    def close(self):
        if self.con:
            self.con.commit()
            self.con.close()
            logging.info("Close database {} successfully".format(self.dbname))
        else:
            logging.info(
                "DataBase doesn't connect,close connection error;please check the db config."
            )

    def getNodeByTaxID(self, id):
        try:
            sql = f"select * from taxon_name where tax_id={id}"
            # print("getNodeByTaxID",sql)
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
            return res
        except:
            return None

    def getMaxTaxonID(self):
        try:
            sql = "select max(tax_id) as max_id from taxon_name;"
            # print(sql)
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
            return res.get("max_id", None)
        except:
            return None


@dataclass
class taxnode:  # ignored
    taxon_sciname: str
    parent_tax_name: Optional[str]
    taxon_id: str
    parent_taxon_id: str
    rank: str
    embl_code: str
    division_id: str
    inherited_div_flag: str
    genetic_code_id: str
    inherited_gc_flag: str
    mitochondrial_genetic_code_id: str
    inherited_mgc_flag: str
    GenBank_hidden_flag: str
    hidden_subtree_root_flag: str
    comments: str
    scientific_name: str
    common_Names: Optional[str]
    synonym_Names: Optional[str]
    fake_taxon_id: Optional[str] = None

    def __repr__(self) -> str:
        return f"""
            taxon_sciname: {self.taxon_sciname}
            parent_tax_name: {self.parent_tax_name}
            taxon_id: {self.taxon_id}
            parent_taxon_id: {self.parent_taxon_id}
            rank: {self.rank}
            embl_code: {self.embl_code}
            division_id: {self.division_id}
            inherited_div_flag: {self.inherited_div_flag}
            genetic_code_id: {self.genetic_code_id}
            inherited_gc_flag: {self.inherited_gc_flag}
            mitochondrial_genetic_code_id: {self.mitochondrial_genetic_code_id}
            inherited_mgc_flag: {self.inherited_mgc_flag}
            GenBank_hidden_flag: {self.GenBank_hidden_flag}
            hidden_subtree_root_flag: {self.hidden_subtree_root_flag}
            comments: {self.comments}
            scientific_name: {self.scientific_name}
            common_Names: {self.common_Names}
            synonym_Names: {self.synonym_Names}
        """


class myutils:

    @classmethod
    def checkStrEmpty(cls, s):
        if str(s).strip() == "":
            return True
        else:
            return False

    @classmethod
    def checkTaxonIDFormat(cls, s):
        if str(s).startswith("A"):
            return True
        else:
            try:
                int(s)
                return True
            except:
                return False

    @classmethod
    def checkParentTaxonID(cls, taxon_id_list, parent_taxon_id_list):
        bad_list = []
        for pp in parent_taxon_id_list:
            if pp not in taxon_id_list:
                if str(pp).startswith("A"):
                    bad_list.append(pp)
        return bad_list

    @classmethod
    def ncbi2node(cls, ncbi_node):
        return taxnode(
            taxon_sciname=str(ncbi_node["name_txt"]),
            parent_tax_name=None,
            taxon_id=str(ncbi_node["taxon_id"]),
            parent_taxon_id=str(ncbi_node["parent_taxon_id"]),
            rank=str(ncbi_node["rank"]),
            embl_code=str(ncbi_node["embl_code"]),
            division_id=str(ncbi_node["division_id"]),
            inherited_div_flag=str(ncbi_node["inherited_div_flag"]),
            genetic_code_id=str(ncbi_node["nuc_gcode"]),
            inherited_gc_flag=str(ncbi_node["inherited_gc_flag"]),
            mitochondrial_genetic_code_id=str(ncbi_node["inherited_mgc_flag"]),
            inherited_mgc_flag=str(ncbi_node["inherited_mgc_flag"]),
            GenBank_hidden_flag=str(ncbi_node["GenBank_hidden"]),
            hidden_subtree_root_flag=str(ncbi_node["hidden_subtree"]),
            comments=str(ncbi_node["comments"]),
            scientific_name=str(ncbi_node["name_txt"]),
            common_Names=None,
            synonym_Names=None)

    @classmethod
    def getParent(cls, this: str, all: Dict[str, taxnode]):
        re = None
        this_node = all.get(this)
        parent_id = this_node.parent_taxon_id
        if parent_id is None or cls.checkStrEmpty(parent_id):
            re = None
        else:
            for x in all.values():
                if parent_id == x.taxon_id:
                    re = x
        return re

    @classmethod
    def getSQLTaxName(cls, node: taxnode):
        # sql = f"""
        #     INSERT INTO `big_gsav2_20190729_1`.`taxon_name`(
        #         `tax_id`,
        #         `name_txt`,
        #         `unique_name`,
        #         `name_class`,
        #         `sample_type_id`)
        #         VALUES (
        #             {node.taxon_id},
        #             "{node.taxon_sciname}",
        #             NULL,
        #             "scientific name",
        #             0);
        #     """
        sql = f"""INSERT INTO `big_gsav2_20190729_1`.`taxon_name`( `tax_id`,  `name_txt`,  `unique_name`,  `name_class`,  `sample_type_id`)  VALUES (  {node.taxon_id},  "{node.taxon_sciname}",  NULL,  "scientific name",  0);"""
        return sql

    @classmethod
    def getSQLTaxNode(cls, node: taxnode):
        # sql = f"""
        # INSERT INTO `big_gsav2_20190729_1`.`taxon_node`(

        #     `tax_id`,
        #     `p_tax_id`,
        #     `ranks`,
        #     `embl_code`,
        #     `divided_id`,
        #     `inherited_div`,
        #     `gencode_id`,
        #     `inherited_gc`,
        #     `mitochondrial_gencode_id`,
        #     `inherited_mgc`,
        #     `genbank_hidden`,
        #     `hidden_subtree_root`,
        #     `comments`,
        #     `plastid_gencode_id`,
        #     `inherited_PGC_flag`,
        #     `specified_species`,
        #     `hydrogenosome_gencode_id`,
        #     `inherited_HGC_flag`) VALUES (
        #         {node.taxon_id},
        #         {node.parent_taxon_id},
        #         '{node.rank}',
        #         '{node.embl_code}',
        #         {node.division_id},
        #         {'0x31' if node.inherited_div_flag == '1' else '0x30'},
        #         {node.genetic_code_id},
        #         {'0x31' if node.inherited_gc_flag == '1' else '0x30'},
        #         {node.mitochondrial_genetic_code_id},
        #         {'0x31' if node.inherited_mgc_flag == '1' else '0x30'},
        #         {'0x31' if node.GenBank_hidden_flag == '1' else '0x30'},
        #         {'0x31' if node.hidden_subtree_root_flag == '1' else '0x30'},
        #         '{node.comments}',
        #         '',
        #         0x00,
        #         '0',
        #         '0',
        #         0x31);
        # """
        sql = f"""INSERT INTO `big_gsav2_20190729_1`.`taxon_node`( `tax_id`,  `p_tax_id`,  `ranks`,  `embl_code`,  `divided_id`,  `inherited_div`,  `gencode_id`,  `inherited_gc`,  `mitochondrial_gencode_id`,  `inherited_mgc`,  `genbank_hidden`,  `hidden_subtree_root`,  `comments`,  `plastid_gencode_id`,  `inherited_PGC_flag`,  `specified_species`,  `hydrogenosome_gencode_id`,  `inherited_HGC_flag`) VALUES (  {node.taxon_id},  {node.parent_taxon_id},  '{node.rank}',  '{node.embl_code}',  {node.division_id},  {'0x31' if node.inherited_div_flag == '1' else '0x30'},  {node.genetic_code_id},  {'0x31' if node.inherited_gc_flag == '1' else '0x30'},  {node.mitochondrial_genetic_code_id},  {'0x31' if node.inherited_mgc_flag == '1' else '0x30'},  {'0x31' if node.GenBank_hidden_flag == '1' else '0x30'},  {'0x31' if node.hidden_subtree_root_flag == '1' else '0x30'},  '{node.comments}',  '',  0x00,  '0',  '0',  0x31);"""
        return sql

    @classmethod
    def printLinage(cls, linage: List[taxnode]):

        if len(linage) == 1:
            return str(linage[0].taxon_id)

        out = ""

        linage_dict_parenttaxonid = {}
        linage_dict_taxonid = {}
        for x in linage:
            linage_dict_parenttaxonid[x.parent_taxon_id] = x
            linage_dict_taxonid[x.taxon_id] = x
        this = linage[0]
        while True:
            if this.taxon_id not in linage_dict_parenttaxonid.keys():
                break
            else:
                this = linage_dict_parenttaxonid[this.taxon_id]

        while True:
            if this.parent_taxon_id not in linage_dict_taxonid.keys():
                break
            else:
                if out == "":
                    out += f"{this.taxon_id} -> {this.parent_taxon_id} "
                else:
                    out += f"-> {this.parent_taxon_id} "
                this = linage_dict_taxonid[this.parent_taxon_id]
        return out

    @classmethod
    def clean_taxname(cls, n):
        return n.strip().lower().replace(" ", "")


parser = argparse.ArgumentParser(prog="parse tax for gsa import")

parser.add_argument("-i", help="input table", required=True)
parser.add_argument("-o", help="output prefix", required=True)
parser.add_argument(
    "-r",
    "--gcoderef",
    required=True,
    help="gcode reference. This file is generated by run: 'python meta2fasta.py build  -n taxdump/names.dmp -c taxdump/nodes.dmp -o gcode_ref'"
)

args = parser.parse_args()

if not os.path.exists(args.i):
    print(f"File does not exists:{args.i}", file=sys.stderr)
    exit(1)


class OUTPUT:

    def __init__(self, outprefix) -> None:

        self.out_sql_path = outprefix + ".sql"
        self.out_log_path = outprefix + ".log"
        self.out_err_path = outprefix + ".err"

        self.out_sql = open(self.out_sql_path, "w")
        self.out_log = open(self.out_log_path, "w")
        self.out_err = open(self.out_err_path, "w")

    def add_sql(self, s):
        if not str(s).endswith("\n"):
            ss = s + "\n"
        else:
            ss = s
        self.out_sql.write(ss)
        self.out_sql.flush()

    def add_log(self, s):
        if not str(s).endswith("\n"):
            ss = s + "\n"
        else:
            ss = s
        self.out_log.write(ss)
        self.out_log.flush()

    def add_err(self, s):
        if not str(s).endswith("\n"):
            ss = s + "\n"
        else:
            ss = s
        self.out_err.write(ss)
        self.out_err.flush()


outfile = OUTPUT(args.o)

header = [
    "taxon_sciname", "parent_tax_name", "taxon_id", "parent_taxon_id", "rank",
    "embl_code", "division_id", "inherited_div_flag", "genetic_code_id",
    "inherited_gc_flag", "mitochondrial_genetic_code_id", "inherited_mgc_flag",
    "GenBank_hidden_flag", "hidden_subtree_root_flag", "comments",
    "scientific_name", "common_Names", "synonym_Names"
]
head_len = len(header)
lien_count = 0
data = {}
taxon_id_set = set()
parent_taxon_id_set = set()
gcodedb = db(args.gcoderef)
print("\n\n** step 1: loading data...")
outfile.add_log("\n\n** step 1: loading data...")
with open(args.i) as f:
    h = next(f)
    for ll in f:
        lien_count += 1
        line = ll.strip("\n").split("\t")
        if len(line) != head_len:
            print(
                f"The {lien_count}th line does not has same column as header ({len(line)} vs {head_len})",
                file=sys.stderr)
            outfile.add_err(
                f"The {lien_count}th line does not has same column as header ({len(line)} vs {head_len})"
            )

            exit(1)
        # node = taxnode(*line)
        node = taxnode(taxon_sciname=line[0],
                       parent_tax_name=None,
                       taxon_id=line[2],
                       parent_taxon_id=line[3],
                       rank=line[4],
                       embl_code=line[5],
                       division_id=line[6],
                       inherited_div_flag=line[7],
                       genetic_code_id=line[8],
                       inherited_gc_flag=line[9],
                       mitochondrial_genetic_code_id=line[10],
                       inherited_mgc_flag=line[11],
                       GenBank_hidden_flag=line[12],
                       hidden_subtree_root_flag=line[13],
                       comments=line[14],
                       scientific_name=line[15],
                       common_Names=None,
                       synonym_Names=None,
                       fake_taxon_id=line[2])

        if not myutils.checkTaxonIDFormat(node.taxon_id):
            print(
                f"Error detected!\nThe format of taxon id should be either starts with A or pure integer.\n'{node.taxon_id}'detected"
            )
            outfile.add_err(
                f"Error detected!\nThe format of taxon id should be either starts with A or pure integer.\n'{node.taxon_id}'detected"
            )
            exit(1)

        data[node.taxon_id] = node
        taxon_id_set.add(node.taxon_id)
        parent_taxon_id_set.add(node.parent_taxon_id)

parent_taxonid_bad_list = myutils.checkParentTaxonID(taxon_id_set,
                                                     parent_taxon_id_set)
if len(parent_taxonid_bad_list) > 0:
    print(
        f"User self-defined taxon id present in parent_taxon_id column but not found in taxon_id.\n{parent_taxonid_bad_list}"
    )
    outfile.add_err(
        f"User self-defined taxon id present in parent_taxon_id column but not found in taxon_id.\n{parent_taxonid_bad_list}"
    )
    exit(1)
print("\n\n** step 2: get lineage start node...")
outfile.add_log("\n\n** step 2: get lineage start node...")
start_nodes_tax_ids = [x for x in taxon_id_set if x not in parent_taxon_id_set]

print("\n\n** step 3: construct local lineage...")
outfile.add_log("\n\n** step 3: construct local lineage...")
linage1 = []
for x in start_nodes_tax_ids:
    sub_linage = [data.get(x)]
    this = x
    while True:
        parent = myutils.getParent(this, data)  # type: ignore
        if parent != None:
            sub_linage.append(parent)
            this = parent.taxon_id
        else:
            break
    linage1.append(sub_linage)

# Find the NCBI parent node
print("\n\n** step 4: construct NCBI lineage...")
outfile.add_log("\n\n** step 4: construct NCBI lineage...")
# api = API()
gsadb = GSAUtils()

linage2 = []

# [[node1, node2], [node3]]

bad_taxname = []
for sublinage1 in linage1:

    # 检查每个item的taxname如果是在NCBI的库中存在，但是被指定了了A开头的编号，则报错。
    for node in sublinage1:
        if gcodedb.fetchOneOrganismByName(
                myutils.clean_taxname(node.taxon_sciname)) is not None:
            if str(node.taxon_id).startswith("A") or str(
                    node.taxon_id).startswith("a"):
                bad_taxname.append([node.taxon_sciname, node.taxon_id])

if len(bad_taxname) > 0:
    res_str = "\n".join([str(x[0]) + ":" + str(x[1]) for x in bad_taxname])
    print(
        f"Error detected!\nFound tax name in NCBI but with a custom id (starts with A)\n{res_str}"
    )
    outfile.add_err(
        f"Error detected!\nFound tax name in NCBI but with a custom id (starts with A)\n{res_str}"
    )
    exit(1)

for sublinage1 in linage1:
    sublinage2 = []
    arrow = 0

    # 处理lineage1的每个sublinage
    while True:
        if arrow >= len(sublinage1):
            break
        node = sublinage1[arrow]
        # 先看父节点在NCBI中有没有，直接接入NCBI,必须要有NCBI的才可以乡下进行。

        # gcodedb.fetchOneOrganismByName()

        parent_in_ncbi = gcodedb.fetchOneOrganismByTaxid(node.parent_taxon_id)

        if parent_in_ncbi is not None:
            # 通过myutils.ncbi2node 函数把从gcoderef中查到的NCBI的节点转化为统一的taxnode节点
            parent_in_ncbi_node = myutils.ncbi2node(parent_in_ncbi)
            sublinage2.append(node)
            sublinage2.append(parent_in_ncbi_node)
            next_parent = parent_in_ncbi_node
            while True:

                pp = gcodedb.fetchOneOrganismByTaxid(
                    next_parent.parent_taxon_id)

                if pp is None:
                    print(
                        f"Error detected!\nCan not found parent node for a valid taxon node in NCBI:{next_parent.taxon_id} | {next_parent.taxon_sciname}"
                    )
                    outfile.add_err(
                        f"Error detected!\nCan not found parent node for a valid taxon node in NCBI:{next_parent.taxon_id} | {next_parent.taxon_sciname}"
                    )
                    exit(1)
                pp_node = myutils.ncbi2node(pp)
                if pp_node.parent_taxon_id != "1":
                    sublinage2.append(pp_node)
                    next_parent = pp_node
                else:
                    sublinage2.append(pp_node)
                    break
            break
        else:
            sublinage2.append(node)
            arrow += 1

    linage2.append(sublinage2)

print(f"Detected {len(linage2)} lineages")
outfile.add_log(f"Detected {len(linage2)} lineages")
for sublinage2 in linage2:
    print(myutils.printLinage(sublinage2))
    outfile.add_log(myutils.printLinage(sublinage2))
# 上一步获得了所有的NCBI lineage ，下一步找到GSA中没有的node，并且构建lineage
print(
    "\n\n** step 5: subset lineage that are present in NCBI but not in GSA...")
outfile.add_log(
    "\n\n** step 5: subset lineage that are present in NCBI but not in GSA...")
linage3 = []
for sublinage2 in linage2:
    sublinage3 = []
    for one_node in sublinage2:

        if one_node.taxon_id.startswith("A"):
            # 新加物种且在NCBI中不存在，一定要输出SQL
            sublinage3.append(one_node)
        else:
            hit_in_gsa = gsadb.getNodeByTaxID(one_node.taxon_id)
            if hit_in_gsa is None:
                sublinage3.append(one_node)
    linage3.append(sublinage3)
print(f"Tax nodes that not present in GSA from {len(linage3)} lineages")
outfile.add_log(
    f"Tax nodes that not present in GSA from {len(linage3)} lineages")
for sublinage in linage3:
    print(myutils.printLinage(sublinage))
    outfile.add_log(myutils.printLinage(sublinage))

print("\n\n** step 6: assign fake id for user self-defined nodes...")
outfile.add_log("\n\n** step 6: assign fake id for user self-defined nodes...")
max_taxon_id = gsadb.getMaxTaxonID()
if max_taxon_id is None:
    print(
        "Error detected!\nCan not allocate max taxon id in GSA database. Please try it later or check the connection."
    )
    outfile.add_err(
        "Error detected!\nCan not allocate max taxon id in GSA database. Please try it later or check the connection."
    )
    exit(1)
fake_taxonid_to_readlGSA = {}
for fakeid in taxon_id_set:
    max_taxon_id += 1
    fake_taxonid_to_readlGSA[fakeid] = max_taxon_id

for sublinage3 in linage3:
    for node in sublinage3:
        if str(node.taxon_id).startswith("A"):
            node.taxon_id = fake_taxonid_to_readlGSA.get(node.taxon_id)
        if str(node.parent_taxon_id).startswith("A"):
            node.parent_taxon_id = fake_taxonid_to_readlGSA.get(
                node.parent_taxon_id)

print("\n\n** step 7: generate SQL...")
outfile.add_log("\n\n** step 7: generate SQL...")
dedup_lineage = {}
for sublinage3 in linage3:
    for node in sublinage3:
        if node.taxon_id not in dedup_lineage.keys():
            dedup_lineage[node.taxon_id] = node
for node in dedup_lineage.values():
    print(f"\n#====>> {node.fake_taxon_id} --> {node.taxon_id}")
    print(myutils.getSQLTaxName(node))
    print(myutils.getSQLTaxNode(node))
    outfile.add_sql(f"\n#====>> {node.fake_taxon_id} --> {node.taxon_id}")
    outfile.add_sql(myutils.getSQLTaxName(node))
    outfile.add_sql(myutils.getSQLTaxNode(node))


print(
    "\n\n** step 8: review [user-local taxon id] --> [GSA local taxon-id]...")
outfile.add_log(
    "\n\n** step 8: review [user-local taxon id] --> [GSA local taxon-id]...")
for k, v in fake_taxonid_to_readlGSA.items():
    print(f"[{k}] --> [{v}]")
    outfile.add_log(f"[{k}] --> [{v}]")
