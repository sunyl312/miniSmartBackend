#!/usr/bin/env python
# coding: utf-8

import pymysql
from xml.dom import minidom
import datetime
import logging
import argparse
import sys
import json
from UserPackage import UserPackage
from Sqlite3Utilis import sqlite3db

logging.basicConfig(level=logging.INFO,
                    format="[%(levelname)s %(asctime)s] %(message)s")

sq = sqlite3db("GSA2SRA.db")


class ComplexEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


class mysqlUtils():

    def __init__(self):

        userinfo = UserPackage("gsadb")
        user, passwd, host, port, name = userinfo['username'], userinfo[
            "password"], userinfo["host"], userinfo["port"], userinfo["dbname"]

        self.host = host
        self.port = port
        self.username = user
        self.password = passwd
        self.dbname = name
        self.charsets = "UTF8"

        try:
            self.con = pymysql.Connect(
                host=self.host,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.dbname,
                charset=self.charsets
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
            self.con.ping(reconnect=True)
            self.cursor.execute(sql)
            return (self.cursor.rowcount)
        else:
            logging.debug(sql)
            logging.debug(str(args))
            self.con.ping(reconnect=True)
            self.cursor.execute(sql, args)
            return (self.cursor.rowcount)

    def excute_insert(self, sql, myargs):
        # sql2 =  pymysql.converters.escape_string(sql) # pymysql version must older than 1.0; from pymysql import escape_string is used for version under 1.0
        # logging.info(sql+"\nvalues:"+",".join(list(myargs)))
        logging.info(sql)
        self.con.ping(reconnect=True)
        self.cursor.execute(sql, myargs)
        return (self.cursor.rowcount)

    def commit(self):
        self.con.commit()

    def select_contact_by_cra_acc(self, cra):
        sql = "select first_name,last_name,email,organization from user where user.user_id=(select user_id from cra where cra.accession=(%s));"
        self.excute(sql, cra)
        return (self.fetchOne())

    def select_bioproject_by_cra_accession(self, cra):
        # .format(country.strip().strip("'").strip())
        sql = "select * from project WHERE project.prj_id = (select prj_id from cra WHERE cra.accession = (%s));"
        self.excute(sql, cra)
        return (self.fetchOne())

    def select_sample_scope_name_by_cra_accession(self, cra):
        # .format(country.strip().strip("'").strip())
        sql = "select s.sample_scope_name from prj_sample_scope s WHERE s.sample_scope_id = (select p.sample_scope_id from project p WHERE p.prj_id = (select prj_id from cra WHERE cra.accession = (%s))); "
        self.excute(sql, cra)
        res = self.fetchOne()
        sra_scope_name = ["Monoisolate", "Multiisolate",
                          "Multispecies", "Environment", "Synthetic"]

        if res['sample_scope_name'] in sra_scope_name:
            return ("e"+res['sample_scope_name'])
        elif res['sample_scope_name'] == 'Single cell':
            return ("eSingleCell")
        else:
            return ("eOther")

    def select_biosampleAndExperimentAndRUN_by_cra_accession(self, cra):
        sql = """
            select  rdf.run_file_id as rdf_run_file_id, rdf.run_file_name as rdf_run_file_name, rdf.archived_file_name as rdf_archive_file_name,rdf.md5 as rdf_md5, ser.* from run_data_file as rdf join (
            select r.exp_id as run_exp_id,r.run_id as run_run_id, r.accession as run_acc, r.run_data_type_id as run_data_type,r.alias as run_alias,se.* from run r join 
            (
            select e.*,s.accession as sam_acc,s.prj_id as sam_prj_id, s.sample_id as sam_sample_id, s.`name` as sam_name, s.title as sam_title,s.taxon_id as sam_taxon_id, s.sample_type_id as sam_sample_type_id, s.public_description as sam_public_description from sample as s join 
            (SELECT experiment.prj_id as exp_prj_id , experiment.cra_id as exp_cra_id, experiment.sample_id as exp_sample_id, experiment.exp_id as exp_exp_id, experiment.selection_id as exp_selection_id, experiment.platform_id as exp_platform_id, experiment.strategy_id as exp_strategy_id, experiment.source_id as exp_source_id, experiment.lib_design as exp_lib_design, experiment.lib_layout as exp_lib_layout , experiment.lib_name as exp_lib_name, experiment.title as exp_title from  experiment  WHERE experiment.cra_id =  (select cra_id from cra WHERE cra.accession = (%s))) as e on s.sample_id = e.exp_sample_id
            ) as se on r.exp_id = se.exp_exp_id
            ) as ser on rdf.run_id = ser.run_run_id where status=10 and run_data_type_id in (1,2);
        """  # 20230928 添加了and run_data_type_id in (1,2)，这样只保证交换fq和bam，也只转这部分得元信息
        self.excute(sql, cra)
        res = self.fetchAll()

        if res == None:
            return []
        else:

            CRR_dict = {}
            for z in res:
                k = z['run_acc']
                if k not in CRR_dict.keys():
                    CRR_dict[k] = {m: n for m, n in z.items() if not str(
                        m).strip().startswith("rdf")}
                    CRR_dict[k]['file_list'] = [
                        {m: n for m, n in z.items() if str(
                            m).strip().startswith("rdf")}
                    ]
                else:
                    CRR_dict[k]['file_list'].append(
                        {m: n for m, n in z.items() if str(
                            m).strip().startswith("rdf")}
                    )
            for crrid in list(CRR_dict.keys()):
                CRR_dict[crrid]['formatted_attrs'] = {}

                # instrument model
                sql_instrument = 'select exp_platform.platform_name from exp_platform WHERE exp_platform.platform_id =(%s);'
                self.excute(sql_instrument, CRR_dict[crrid]['exp_platform_id'])
                sql_instrument_res = self.fetchOne()
                CRR_dict[crrid]['formatted_attrs']['instrument_model'] = sql_instrument_res['platform_name']

                # library_source
                sql_source = 'select exp_lib_source.source_name from exp_lib_source WHERE exp_lib_source.source_id =(%s);'
                self.excute(sql_source, CRR_dict[crrid]['exp_source_id'])
                sql_source_res = self.fetchOne()
                CRR_dict[crrid]['formatted_attrs']['library_source'] = sql_source_res['source_name']

                # library_selection
                sql_selection = 'select exp_lib_selection.selection_name from exp_lib_selection WHERE exp_lib_selection.selection_id =(%s);'
                self.excute(sql_selection, CRR_dict[crrid]['exp_selection_id'])
                selection_res = self.fetchOne()
                CRR_dict[crrid]['formatted_attrs']['library_selection'] = selection_res['selection_name']

                # library_strategy
                sql_strategy = 'select exp_lib_strategy.strategy_name from exp_lib_strategy WHERE exp_lib_strategy.strategy_id =(%s);'
                self.excute(sql_strategy, CRR_dict[crrid]['exp_strategy_id'])
                strategy_res = self.fetchOne()
                CRR_dict[crrid]['formatted_attrs']['library_strategy'] = strategy_res['strategy_name']

                # library_name
                CRR_dict[crrid]['formatted_attrs']['library_name'] = "missing" if str(
                    CRR_dict[crrid]['exp_lib_name']) == "None" else str(CRR_dict[crrid]['exp_lib_name'])

                # library_protocol
                CRR_dict[crrid]['formatted_attrs']['library_construction_protocol'] = "missing" if CRR_dict[crrid]['exp_lib_design'] == None else str(
                    CRR_dict[crrid]['exp_lib_design'])

                # library layout
                CRR_dict[crrid]['formatted_attrs']['library_layout'] = "SINGLE" if str(
                    CRR_dict[crrid]['exp_lib_layout']).strip() == "1" else "PAIRED"

            return (CRR_dict.values())

    def select_prj_data_type_by_cra_accession(self, cra):
        # .format(country.strip().strip("'").strip())
        sql = "select x.data_type_name from prj_data_type x WHERE x.data_type_id IN   (select d.data_type_id from pro_data_type d WHERE d.prj_id =  (select prj_id from cra WHERE cra.accession = (%s)));"
        self.excute(sql, cra)
        res = self.fetchAll()
        logging.debug("select_prj_data_type_by_cra_accession:{}".format(res))
        if res == None:
            return []
        else:

            data_type_mapping = {
                'GENOME SEQUENCING': 'genome sequencing',
                'RAW SEQUENCE READS': 'raw sequence reads',
                'GENOME SEQUENCING AND ASSEMBLY': 'genome sequencing and assembly',
                'METAGENOME': 'metagenome',
                'METAGENOMIC ASSEMBLY': 'metagenomic assembly',
                'ASSEMBLY': 'assembly',
                'TRANSCRIPTOME': 'transcriptome',
                'PROTEOMIC': 'proteomic',
                'MAP': 'map',
                'CLONE ENDS': 'clone ends',
                'ATRGETED LOCI': 'targeted loci',
                'TARGETED LOCI CULTURED': 'targeted loci cultured',
                'TARGETED LOCI ENVIRONMENTAL': 'targeted loci environmental',
                'RANDOM SURVEY': 'random survey',
                'EXOME': 'exome',
                'VARIATION': 'variation',
                'EPIGENOMICS': 'epigenomics',
                'PHENOTYPE OR GENOTYPE': 'phenotype or genotype'
            }
            formated_dt = []
            hasOther = False
            for x in res:
                if x['data_type_name'].strip() in data_type_mapping.keys():
                    formated_dt.append({
                        "data_type_name": data_type_mapping[x['data_type_name'].strip()]
                    })
                else:
                    hasOther = True
            if hasOther:
                formated_dt.append({
                    "data_type_name": "other"
                })

            return formated_dt

    def select_taxon_name_by_cra_accession(self, cra):
        sql = "select k.name_txt from (select * from taxon_name WHERE taxon_name.tax_id IN  (select sample.taxon_id from sample WHERE sample.sample_id IN (SELECT experiment.sample_id from  experiment  WHERE experiment.cra_id =  (select cra_id from cra WHERE cra.accession = (%s)))))  as k  WHERE k.name_class ='scientific name';"
        self.excute(sql, cra)
        res = self.fetchAll()
        logging.debug("select_taxon_name_by_cra_accession:{}".format(res))
        if res == None:
            return []
        else:

            return res

    def select_taxon_name_by_taxonid(self, taxid):
        sql = "SELECT name_txt FROM taxon_name WHERE taxon_name.tax_id = (%s) and taxon_name.name_class = 'scientific name'"
        self.excute(sql, taxid)
        res = self.fetchOne()
        return res

    def select_samples_with_attrs_by_cra_accession(self, cra):
        sql = "select * from sample WHERE sample.sample_id IN (SELECT experiment.sample_id from  experiment  WHERE experiment.cra_id =  (select cra_id from cra WHERE cra.accession = (%s)));"
        self.excute(sql, cra)
        samples_list = self.fetchAll()
        logging.debug(type(samples_list[0]))
        logging.debug(
            "select_samples_with_attrs_by_cra_accession:{}".format(samples_list))

        if samples_list == None:
            return []
        else:
            sample_process_dict = {x['sample_id']: x for x in list(samples_list)}

            sample_type_mapping = {"1": "sample_attr_pathogen_clinical_host_associated",
                                   "2": "sample_attr_pathogen_environmental_food_other",
                                   "3": "sample_attr_microbe",
                                   "4": "sample_attr_model_animal",
                                   "5": "sample_attr_human",
                                   "6": "sample_attr_plant",
                                   "7": "sample_attr_virus",
                                   "8": "sample_attr_metagenome_environmental",
                                   "9": "sample_attr_mimsme_human_gut",
                                   "10": "sample_attr_mimsme_soil",
                                   "11": "sample_attr_mimsme_water"}

            package_mapping = {"1": "Pathogen.cl.1.0",
                               "2": "Pathogen.env.1.0",
                               "3": "Microbe.1.0",
                               "4": "Model.organism.animal.1.0",
                               "6": "Plant.1.0",
                               "7": "Virus.1.0",
                               "8": "Metagenome.environmental.1.0",
                               "9": "MIMS.me.human-gut.5.0",
                               "10": "MIMS.me.soil.5.0",
                               "11": "MIMS.me.water.5.0"}

            sample_ids = list(sample_process_dict.keys())

            for k in sample_ids:
                logging.debug("sample_process_dict[k]:{}".format(
                    sample_process_dict[k]['sample_type_id']))
                sample_type_table_name = sample_type_mapping[str(
                    sample_process_dict[k]['sample_type_id'])]

                # query attrs in sample_attr_tables
                sql2 = "select * from  {} as t  WHERE t.sample_id = '{}';".format(
                    sample_type_table_name, sample_process_dict[k]['sample_id'])
                self.excute(sql2)
                one_sample_attr = self.fetchOne()

                filtered_attrs = {k: v for k, v in one_sample_attr.items() if k not in [
                    'sample_id', 'type', 'taxon_id', "attribute_id", "geographic_location", "latitude_longitude"]}

                sample_process_dict[k]['attrs'] = filtered_attrs
                sample_process_dict[k]['package'] = package_mapping[str(
                    sample_process_dict[k]['sample_type_id'])]

                # query taxon id
                sql_taxon = "select k.name_txt from taxon_name k WHERE k.tax_id = (%s) and k.name_class ='scientific name';"
                self.excute(sql_taxon, sample_process_dict[k]['taxon_id'])
                one_sample_taxon_name = self.fetchOne()
                sample_process_dict[k]['taxon_name'] = one_sample_taxon_name
            logging.debug("dumped samples with attr:{}".format(
                sample_process_dict, indent=4))
            # print( json.dumps(list(sample_process_dict.values()),indent=4,cls=ComplexEncoder))
            return sample_process_dict.values()

    def generate_project_relevance(self):
        sql = '''
            select distinct(relevance) from project;
            '''
        self.excute(sql)
        project_relevance = self.fetchAll()
        gsa2sra_project_relevance = {}
        for i in project_relevance:
            relevance_name = str(
                i["relevance"]).strip().replace(" ", "").upper()
            if relevance_name == "AGRICULTURAL":
                gsa2sra_project_relevance[i["relevance"]] = "Agricultural"
            elif relevance_name == "MEDICAL":
                gsa2sra_project_relevance[i["relevance"]] = "Medical"
            elif relevance_name == "INDUSTRIAL":
                gsa2sra_project_relevance[i["relevance"]] = "Industrial"
            elif relevance_name == "ENVIRONMENTAL":
                gsa2sra_project_relevance[i["relevance"]] = "Environmental"
            elif relevance_name == "EVOLUTION":
                gsa2sra_project_relevance[i["relevance"]] = "Evolution"
            elif relevance_name == "MODELORGANISM":
                gsa2sra_project_relevance[i["relevance"]] = "ModelOrganism"
            elif relevance_name == "OTHER":
                gsa2sra_project_relevance[i["relevance"]] = "Other"
            # else:
            #     gsa2sra_project_relevance[i["relevance"]]="Other"
        return gsa2sra_project_relevance


def generate_description(cra_accession, release_date, first_name, last_name, email, organization1):

    FirstName = doc.createElement('First')
    FirstName.appendChild(doc.createTextNode(first_name))

    LastName = doc.createElement('Last')
    LastName.appendChild(doc.createTextNode(last_name))

    Name = doc.createElement("Name")
    Name.appendChild(FirstName)
    Name.appendChild(LastName)

    Contact = doc.createElement("Contact")
    Contact.appendChild(Name)
    Contact.setAttribute("email", email)

    organisation_name = doc.createElement("Name")
    organisation_name.appendChild(doc.createTextNode(organization1))

    organization = doc.createElement("Organization ")
    organization.setAttribute("role", "owner")
    organization.setAttribute("type", "institute")
    organization.appendChild(organisation_name)
    organization.appendChild(Contact)

    comment = doc.createElement("Comment")
    # comment.appendChild(doc.createTextNode("GSA to SRA {}".format(cra_accession)))
    comment.appendChild(doc.createTextNode(
        "GSA to SRA {}".format(cra_accession)))

    Hold = doc.createElement("Hold ")
    Hold.setAttribute("release_date", release_date)

    Description = doc.createElement("Description")
    Description.appendChild(comment)
    Description.appendChild(organization)
    Description.appendChild(Hold)

    return (Description)


def generate_action_tree(target_db, spuid):
    action = doc.createElement("Action")
    adddata = doc.createElement("AddData")
    adddata.setAttribute("target_db", target_db)
    action.appendChild(adddata)
    data = doc.createElement("Data")
    data.setAttribute("content_type", "xml")
    adddata.appendChild(data)
    XmlContent = doc.createElement("XmlContent ")
    data.appendChild(XmlContent)

    Identifier = doc.createElement("Identifier")

    SPUID = doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(doc.createTextNode(spuid))
    Identifier.appendChild(SPUID)

    adddata.appendChild(Identifier)

    return ([action, adddata, data, XmlContent])


# create  bioproject
def generate_bioproject(target_db, prj_accn, prj_title, prj_description, prj_relevance, prj_sample_scope, prj_data_type, prj_taxon_name):
    prjaction, prjadddata, prjdata, prjxmlcontent = generate_action_tree(
        target_db, prj_accn)

    Project = doc.createElement("Project")
    Project.setAttribute("schema_version", "2.0")
    prjxmlcontent.appendChild(Project)

    # project id
    ProjectID = doc.createElement("ProjectID")
    SPUID = doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(doc.createTextNode(str(prj_accn).strip()))
    ProjectID.appendChild(SPUID)
    Project.appendChild(ProjectID)

    # descriptor
    Descriptor = doc.createElement("Descriptor")
    DesTitle = doc.createElement("Title")
    DesTitle.appendChild(doc.createTextNode(str(prj_title).strip()))
    Descriptor.appendChild(DesTitle)

    DesDescription = doc.createElement("Description")
    DesDescriptionP = doc.createElement("p")
    DesDescriptionP.appendChild(
        doc.createTextNode(str(prj_description).strip()))
    DesDescription.appendChild(DesDescriptionP)
    Descriptor.appendChild(DesDescription)

    DesExternalLink = doc.createElement("ExternalLink")
    DesExternalLinkRUL = doc.createElement("URL")
    DesExternalLinkRUL.appendChild(doc.createTextNode(
        "https://ngdc.cncb.ac.cn/bioproject/browse/"+str(prj_accn).strip()))
    DesExternalLink.appendChild(DesExternalLinkRUL)
    Descriptor.appendChild(DesExternalLink)

    mysqlutilis = mysqlUtils()
    # 20230720 gsa中project relevance不对应，导致报错，建立映射关系
    gsa2sra_project_relevance = mysqlutilis.generate_project_relevance()
    DesRelevance = doc.createElement("Relevance")
    # 这里是什么就标签是什么
    DesRelevanceValue = doc.createElement(
        gsa2sra_project_relevance[prj_relevance])
    # DesRelevanceValue.appendChild(doc.createTextNode("Yes")) #把中间内容替换成p_标签，包裹用户原先的内容
    DesRelevanceTag = doc.createElement("p")
    DesRelevanceTag.appendChild(doc.createTextNode(str(prj_relevance)))
    DesRelevanceValue.appendChild(DesRelevanceTag)

    DesRelevance.appendChild(DesRelevanceValue)
    Descriptor.appendChild(DesRelevance)
    Project.appendChild(Descriptor)

    # ProjectType

    ProjectType = doc.createElement("ProjectType")
    ProjectTypeSubmission = doc.createElement("ProjectTypeSubmission")
    ProjectTypeSubmission.setAttribute("sample_scope", prj_sample_scope)

    for o in prj_taxon_name:
        Organism = doc.createElement("Organism")
        OrganismName = doc.createElement("OrganismName")
        OrganismName.appendChild(doc.createTextNode(o['name_txt']))
        Organism.appendChild(OrganismName)
        # IntendedDataTypeSet =  doc.createElement("IntendedDataTypeSet")
        # IntendedDataTypeSet.appendChild(Organism)
        ProjectTypeSubmission.appendChild(Organism)

    for dt in prj_data_type:
        DataType = doc.createElement("DataType")
        DataType.appendChild(doc.createTextNode(dt['data_type_name']))
        IntendedDataTypeSet = doc.createElement("IntendedDataTypeSet")
        IntendedDataTypeSet.appendChild(DataType)
        ProjectTypeSubmission.appendChild(IntendedDataTypeSet)
    ProjectType.appendChild(ProjectTypeSubmission)

    Project.appendChild(ProjectType)

    return (prjaction)

# create Known bioproject


def generate_known_bioproject(PRJNAacc):
    PrjAttributeRefId = doc.createElement("AttributeRefId")
    PrjAttributeRefId.setAttribute("name", "BioProject")
    PrjRefId = doc.createElement("RefId")
    PrjSPUID = doc.createElement("PrimaryId")
    PrjSPUID.setAttribute("db", "BioProject")
    PrjSPUID.appendChild(doc.createTextNode(PRJNAacc))
    PrjRefId.appendChild(PrjSPUID)
    PrjAttributeRefId.appendChild(PrjRefId)
    # print(PrjAttributeRefId)
    return PrjAttributeRefId
    # adddata.appendChild(PrjAttributeRefId)


# biosample

def generate_biosample(target_db, spuid, sample_package, sample_taxonname, sample_with_attr):
    sam_action, sam_adddata, sam_data, sam_xmlcontent = generate_action_tree(
        target_db, spuid)
    BioSample = doc.createElement("BioSample")
    BioSample.setAttribute("schema_version", "2.0")
    sam_xmlcontent.appendChild(BioSample)

    # sample id
    SampleId = doc.createElement("SampleId")
    SPUID = doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(doc.createTextNode(spuid))
    SampleId.appendChild(SPUID)
    BioSample.appendChild(SampleId)

    # Descriptor
    Descriptor = doc.createElement("Descriptor")
    Title = doc.createElement("Title")
    Title.appendChild(doc.createTextNode(sample_with_attr['name']))
    Descriptor.appendChild(Title)
    BioSample.appendChild(Descriptor)

    # Organism
    Organism = doc.createElement("Organism")
    OrganismName = doc.createElement("OrganismName")
    if int(sample_with_attr['sample_type_id']) in [8, 9, 10, 11, 12] and str(sample_taxonname['name_txt']).endswith("metagenome") is False:
        # print("需要执行物种转换")
        if int(sample_with_attr['sample_type_id']) == 8:
            OrganismName.appendChild(doc.createTextNode("metagenome"))
        elif int(sample_with_attr['sample_type_id']) == 9:
            OrganismName.appendChild(
                doc.createTextNode("human gut metagenome"))
        elif int(sample_with_attr['sample_type_id']) == 10:
            OrganismName.appendChild(doc.createTextNode("soil metagenome"))
        elif int(sample_with_attr['sample_type_id']) == 11:
            OrganismName.appendChild(doc.createTextNode("water metagenome"))
    else:
        OrganismName.appendChild(
            doc.createTextNode(sample_taxonname['name_txt']))
    Organism.appendChild(OrganismName)
    BioSample.appendChild(Organism)

    # Package
    Package = doc.createElement("Package")
    Package.appendChild(doc.createTextNode(sample_package))
    BioSample.appendChild(Package)

    # attribute
    Attributes = doc.createElement("Attributes")

    # fake = {
    #     "geo_loc_name":"not collected",
    #     "tissue":"liver",
    #     "cultivar":"not collected",
    #     "age":"42",
    #     }

    GSA2NCBI_attr_mapping = {
        "cultivar": "cultivar",
        "biomaterial_provider": "biomaterial_provider",
        "tissue": "tissue",
        "age": "age",
        "cell_line": "cell_line",
        "cell_type": "cell_type",
        "collected_by": "collected_by",
        "collection_date": "collection_date",
        "culture_collection": "culture_collection",
        "dev_stage": "dev_stage",
        "disease": "disease",
        "disease_stage": "disease_stage",
        "genotype": "genotype",
        "growth_protocol": "growth_protocol",
        "height_length": "height_or_length",
        "isolation_source": "isolation_source",
                            "latitude_longitude": "lat_lon",
                            "phenotype": "phenotype",
                            "population": "population",
                            "sex": "sex",
                            "specimen_voucher": "specimen_voucher",
                            "treatment": "treatment",
                            "isolate": "isolate",
                            "strain": "strain",
                            "host_organism_id": "host",  # 2022年8月4日 update  host_taxid -> host
                            "lab_host": "lab_host",
                            "geographic_location": "geo_loc_name",
                            "altitude": "altitude",
                            "depth": "depth",
                            "host_tissue_sampled": "host_tissue_sampled",
                            "identified_by": "identified_by",
                            "passage_history": "passage_history",
                            "sample_size": "samp_size",
                            "serotype": "serotype",
                            "serovar": "serovar",
                            "subgroup": "subgroup",
                            "subtype": "subtype",
                            "host_disease": "host_disease",
                            "host_age": "host_age",
                            "host_description": "host_description",
                            "host_disease_outcome": "host_disease_outcome",
                            "host_disease_stage": "host_disease_stage",
                            "host_health_state": "host_health_state",
                            "host_sex": "host_sex",
                            "host_subject_id": "host_subject_id",
                            "pathotype": "pathotype",
                            "breed": "breed",
                            "birth_date": "birth_date",
                            "birth_location": "birth_location",
                            "breed_history": "breeding_history",
                            "breed_method": "breeding_method",
                            "cell_subtype": "cell_subtype",
                            "death_date": "death_date",
                            "health_state": "health_state",
                            "storage_conditions": "store_cond",
                            "stud_book_number": "stud_book_number",
                            "elevation": "elev",
                            "agrochemical_additions": "agrochem_addition",
                            "aluminium_saturation": "al_sat",
                            "aluminium_saturation_method": "al_sat_meth",
                            "annual_seasonal_precipitation": "annual_season_precpt",
                            "annual_seasonal_temperature": "annual_season_temp",
                            "crop_rotation": "crop_rotation",
                            "current_vegetation": "cur_vegetation",
                            "current_vegetation_method": "cur_vegetation_meth",
                            "drainage_classification": "drainage_class",
                            "extreme_event": "extreme_event",
                            "extreme_salinity": "extreme_salinity",
                            "fao_classification": "fao_class",
                            "fire": "fire",
                            "flooding": "flooding",
                            "heavy_metals": "heavy_metals",
                            "heavy_metals_method": "heavy_metals_meth",
                            "horizon": "horizon",
                            "horizon_method": "horizon_meth",
                            "links_additional_analysis": "link_addit_analys",
                            "link_classification_information": "link_class_info",
                            "link_climate_information": "link_climate_info",
                            "local_classification": "local_class",
                            "local_classification_method": "local_class_meth",
                            "microbial_biomass": "microbial_biomass",
                            "microbial_biomass_method": "microbial_biomass",
                            "miscellaneous_parameter": "microbial_biomass_meth",
                            "ph": "ph",
                            "ph_method": "ph_meth",
                            "pooling_dna_extracts": "pool_dna_extracts",
                            "previous_land_use": "previous_land_use",
                            "previous_land_use_method": "previous_land_use_meth",
                            "profile_position": "profile_position",
                            "salinity_method": "salinity_meth",
                            "sieving": "sieving",
                            "slope_aspect": "slope_aspect",
                            "soil_type": "soil_type",
                            "slope_gradient": "slope_gradient",
                            "soil_type_method": "soil_type_meth",
                            "texture": "texture",
                            "texture_method": "texture_meth",
                            "tillage": "tillage",
                            "total_n_method": "tot_n_meth",
                            "total_nitrogen": "tot_nitro",
                            "total_organic_carbon_method": "tot_org_c_meth",
                            "total_organic_carbon": "tot_org_carb",
                            "water_content_soil": "water_content_soil",
                            "water_content_soil_method": "water_content_soil_meth",
                            "reference_biomaterial": "ref_biomaterial",
                            "sample_collection_device": "samp_collect_device",
                            "sample_material_processing": "samp_mat_process",
                            "sample_volume_weight_dna_extraction": "samp_vol_we_dna_ext",
                            "source_material_identifiers": "source_material_id",
                            "description": "description",
                            "chemical_administration": "chem_administration",
                            "ethnicity": "ethnicity",
                            "gastrointestinal_tract_disorder": "gastrointest_disord",
                            "host_mass_index": "host_body_mass_index",
                            "host_product": "host_body_product",
                            "host_temperature": "host_body_temp",
                            "host_diet": "host_diet",
                            "host_family_relationship": "host_family_relationship",
                            "host_genotype": "host_genotype",
                            "host_height": "host_height",
                            "host_last_meal": "host_last_meal",
                            "host_occupation": "host_occupation",
                            "host_phenotype": "host_phenotype",
                            "host_pulse": "host_pulse",
                            "host_total_mass": "host_tot_mass",
                            "medication_code": "ihmc_medication_code",
                            "liver_disorder": "liver_disord",
                            "medical_history_performed": "medic_hist_perform",
                            "organism_count": "organism_count",
                            "perturbation": "perturbation",
                            "salinity": "salinity",
                            "sample_storage_duration": "samp_store_dur",
                            "sample_storage_location": "samp_store_loc",
                            "sample_storage_temperature": "samp_store_temp",
                            "special_diet": "special_diet",
                            "mating_type": "mating_type",
                            "alkalinity": "alkalinity",
                            "alkyl_diethers": "alkyl_diethers",
                            "aminopeptidase_activity": "aminopept_act",
                            "ammonium": "ammonium",
                            "atmospheric_data": "atmospheric_data",
                            "bacterial_production": "bac_prod",
                            "bacterial_respiration": "bac_resp",
                            "bacterial_carbon_production": "bacteria_carb_prod",
                            "biomass": "biomass",
                            "bishomohopanol": "bishomohopanol",
                            "bromide": "bromide",
                            "calcium": "calcium",
                            "carbon_nitrogen_ratio": "carb_nitro_ratio",
                            "chloride": "chloride",
                            "chlorophyll": "chlorophyll",
                            "conductivity": "conduc",
                            "density": "density",
                            "diether_lipids": "diether_lipids",
                            "dissolved_carbon_dioxide": "diss_carb_dioxide",
                            "dissolved_hydrogen": "diss_hydrogen",
                            "dissolved_inorganic_carbon": "diss_inorg_carb",
                            "dissolved_inorganic_nitrogen": "diss_inorg_nitro",
                            "dissolved_inorganic_phosphorus": "diss_inorg_phosp",
                            "dissolved_organic_carbon": "diss_org_carb",
                            "dissolved_organic_nitrogen": "diss_org_nitro",
                            "dissolved_oxygen": "diss_oxygen",
                            "downward_par": "down_par",
                            "fluorescence": "fluor",
                            "glucosidase_activity": "glucosidase_act",
                            "light_intensity": "light_intensity",
                            "magnesium": "magnesium",
                            "mean_friction_velocity": "mean_frict_vel",
                            "mean_peak_friction_velocity": "mean_peak_frict_vel",
                            "n_alkanes": "n_alkanes",
                            "nitrate": "nitrate",
                            "nitrite": "nitrite",
                            "nitrogen": "nitro",
                            "organic_carbon": "org_carb",
                            "organic_matter": "org_matter",
                            "organic_nitrogen": "org_nitro",
                            "oxygenation_status": "oxy_stat_samp",
                            "particulate_organic_carbon": "part_org_carb",
                            "particulate_organic_nitrogen": "part_org_nitro",
                            "petroleum_hydrocarbon": "petroleum_hydrocarb",
                            "phaeopigments": "phaeopigments",
                            "phosphate": "phosphate",
                            "phospholipid_fatty_acid": "phosplipid_fatt_acid",
                            "photon_flux": "photon_flux",
                            "potassium": "potassium",
                            "pressure": "pressure",
                            "primary_production": "primary_prod",
                            "redox_potential": "redox_potential",
                            "silicate": "silicate",
                            "sodium": "sodium",
                            "soluble_reactive_phosphorus": "soluble_react_phosp",
                            "sulfate": "sulfate",
                            "sulfide": "sulfide",
                            "suspended_particulate_matter": "suspend_part_matter",
                            "tidal_stage": "tidal_stage",
                            "total_depth_water_column": "tot_depth_water_col",
                            "total_dissolved_nitrogen": "tot_diss_nitro",
                            "total_inorganic_nitrogen": "tot_inorg_nitro",
                            "total_particulate_carbon": "tot_part_carb",
                            "total_phosphorus": "tot_phosp",
                            "water_current": "water_current"}

    GSA2NCBI_attr_sex_mapping = {"1": "male",
                                 "2": "female",
                                 "3": "neuter",
                                 "4": "hermaphrodite",
                                 "5": "not determined",
                                 "6": "missing",
                                 "7": "not applicable",
                                 "8": "not collected"}

    # print()
    mysqlutils = mysqlUtils()
    for k, v in sample_with_attr['attrs'].items():

        NCBIattrName = GSA2NCBI_attr_mapping.get(k, None)

        if NCBIattrName == None:
            continue

        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", NCBIattrName)

        this_attr = "missing"

        if v != None:
            if str(k).lower() == "collection_date":
                if isinstance(v, str):
                    try:
                        this_attr = v.split(" ")[0]
                    except:
                        this_attr = "missing"
                elif isinstance(v, datetime.datetime):
                    this_attr = v.strftime("%Y-%m-%d")
                else:
                    this_attr = "missing"
            elif str(k).lower() == "age":
                this_unit = sample_with_attr['attrs'].get("age_unit", None)
                if this_unit is not None:
                    this_attr = str(v) + " " + this_unit
                else:
                    this_attr = str(v)
            elif str(k).lower() == "host_age":
                this_unit = sample_with_attr['attrs'].get(
                    "host_age_unit", None)
                if this_unit is not None:
                    this_attr = str(v) + " " + this_unit
                else:
                    this_attr = str(v)
            elif str(k).lower() == "host_organism_id":  # 2022年8月4日 这里在查一下表，将taxid 转换taxname

                taxon_name = mysqlutils.select_taxon_name_by_taxonid(v)
                this_attr = str(taxon_name['name_txt'])

            # sex,规则是，把数字替换成后面的字
            # 1 male
            # 2 female
            # 3 neuter
            # 4 hermaphrodite
            # 5 not determined
            # 6 missing
            # 7 not applicable
            # 8 not collected
            elif str(k).strip().lower() == "sex":
                this_attr = GSA2NCBI_attr_sex_mapping.get(str(v), "missing")
            else:
                if str(v).strip() == "":
                    this_attr = "missing"
                else:
                    this_attr = str(v)
        else:
            this_attr = "missing"
        innerT = doc.createTextNode(this_attr)
        attr.appendChild(innerT)
        Attributes.appendChild(attr)

    # Attr信息修改
    # a.microbe（type是3）中需要添加sample_type这个属性值，因为GSA没有这个字段，所以填写为missing。Animal和plant（4和6类型）中需要添加ecotype。因为GSA没有这个字段，所以填写为missing。4.20发现只有ecotype可以不填，但是植物类型中缺少geo_loc_name
    if int(sample_with_attr['sample_type_id']) == 3:
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "sample_type")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
        # attr = doc.createElement("Attribute") #202307这个地方跟下边range(0-12中添加geo_loc_name存在了重复，所以注释掉了)
        # attr.setAttribute("attribute_name","geo_loc_name")
        # attr.appendChild(doc.createTextNode("missing"))
        # Attributes.appendChild(attr)
    if int(sample_with_attr['sample_type_id']) in range(0, 12):
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "geo_loc_name")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "lat_lon")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
    if int(sample_with_attr['sample_type_id']) == 4:
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "collection_date")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
    if int(sample_with_attr['sample_type_id']) == 10:
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "env_broad_scale")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "env_local_scale")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
        attr = doc.createElement("Attribute")
        attr.setAttribute("attribute_name", "env_medium")
        attr.appendChild(doc.createTextNode("missing"))
        Attributes.appendChild(attr)
    # print(sample_with_attr['sample_type_id'])
    # elif int(sample_with_attr['sample_type_id']) == 4 or int(sample_with_attr['sample_type_id']) == 6:
    #     attr = doc.createElement("Attribute")
    #     attr.setAttribute("attribute_name","ecotype")
    #     attr.appendChild(doc.createTextNode("missing"))
    #     Attributes.appendChild(attr)

    BioSample.appendChild(Attributes)

    return (sam_action)

# create Known biosample


def generate_known_biosample(SAMNacc):
    sampleAttributeRefId = doc.createElement("AttributeRefId")
    sampleAttributeRefId.setAttribute("name", "BioSample")
    sampleRefId = doc.createElement("RefId")
    sampleSPUID = doc.createElement("PrimaryId")
    sampleSPUID.setAttribute("db", "BioSample")
    sampleSPUID.appendChild(doc.createTextNode(SAMNacc))
    sampleRefId.appendChild(sampleSPUID)
    sampleAttributeRefId.appendChild(sampleRefId)
    return sampleAttributeRefId


def generate_sra(target_db, projectid, CRR_data):

    action = doc.createElement("Action")
    adddata = doc.createElement("AddFiles")
    adddata.setAttribute("target_db", target_db)
    action.appendChild(adddata)

    # fake_file_list = [
    #     {
    #         "file_path":"CRR087495_f1.fq.gz",
    #         "DataType":"generic-data"
    #     },
    #     {
    #         "file_path":"CRR087495_f2.fq.gz",
    #         "DataType":"generic-data"
    #     },
    # ]

    for f in CRR_data['file_list']:
        oneFile = doc.createElement("File")
        oneFile.setAttribute("file_path", f['rdf_archive_file_name'])
        DataType = doc.createElement("DataType")
        DataType.appendChild(doc.createTextNode("generic-data"))
        oneFile.appendChild(DataType)
        adddata.appendChild(oneFile)

    # fake_attribute_list = {
    #     "instrument_model":"Illumina HiSeq X Ten",
    #     "library_name":"NA",
    #     "library_strategy":"WGS",
    #     "library_source":"GENOMIC",
    #     "library_selection":"size fractionation",
    #     "library_layout":"PAIRED",
    #     "library_construction_protocol":"total genomic DNA was extract from leaves with CTAB methods, shear to ~400bp with Bioruptor plus, library construction using Kit from Vazyme, sequence was generate on Illumina Hiseq x ten sequencer",
    # }

    for k, v in CRR_data['formatted_attrs'].items():
        oneAttr = doc.createElement("Attribute")
        oneAttr.setAttribute("name", k)
        oneAttr.appendChild(doc.createTextNode(v))
        adddata.appendChild(oneAttr)

    try:
        PRJNAacc = sq.FetchAccession("PRJCAacc", projectid)
        if PRJNAacc == None or PRJNAacc["PRJNAacc"] == None or PRJNAacc["PRJNAacc"] == "":
            PrjAttributeRefId = doc.createElement("AttributeRefId")
            PrjAttributeRefId.setAttribute("name", "BioProject")
            PrjRefId = doc.createElement("RefId")
            PrjSPUID = doc.createElement("SPUID")
            PrjSPUID.setAttribute("spuid_namespace", "NGDC")
            PrjSPUID.appendChild(doc.createTextNode(projectid))
            PrjRefId.appendChild(PrjSPUID)
            PrjAttributeRefId.appendChild(PrjRefId)
            adddata.appendChild(PrjAttributeRefId)

        elif str(PRJNAacc["PRJNAacc"]).startswith("PRJNA"):
            PrjAttributeRefId = doc.createElement("AttributeRefId")
            PrjAttributeRefId.setAttribute("name", "BioProject")
            PrjRefId = doc.createElement("RefId")
            PrjSPUID = doc.createElement("PrimaryId")
            PrjSPUID.setAttribute("db", "BioProject")
            PrjSPUID.appendChild(doc.createTextNode(str(PRJNAacc["PRJNAacc"])))
            PrjRefId.appendChild(PrjSPUID)
            PrjAttributeRefId.appendChild(PrjRefId)
            adddata.appendChild(PrjAttributeRefId)
            # print(PrjRefId,"sample中插入project")

            # print(str(PRJNAacc["PRJNAacc"]))
            # PrjAttributeRefId=generate_known_bioproject(str(PRJNAacc["PRJNAacc"]))
            # adddata.appendChild(PrjAttributeRefId)
    except Exception as e:
        print(e)

    try:
        SAMNacc = sq.FetchAccession("SAMCacc", CRR_data['sam_acc'])
        if SAMNacc == None or SAMNacc["SAMNacc"] == None or SAMNacc["SAMNacc"] == "":
            SamAttributeRefId = doc.createElement("AttributeRefId")
            SamAttributeRefId.setAttribute("name", "BioSample")
            SamRefId = doc.createElement("RefId")
            SamSPUID = doc.createElement("SPUID")
            SamSPUID.setAttribute("spuid_namespace", "NGDC")
            SamSPUID.appendChild(doc.createTextNode(
                CRR_data['sam_acc']+": "+CRR_data['sam_name']))
            SamRefId.appendChild(SamSPUID)
            SamAttributeRefId.appendChild(SamRefId)
            adddata.appendChild(SamAttributeRefId)

        else:
            SamAttributeRefId = generate_known_biosample(
                str(SAMNacc["SAMNacc"]))
            adddata.appendChild(SamAttributeRefId)

    except Exception as e:
        print(e)

    # if SAMNacc ==None or SAMNacc=="":
    #     SamAttributeRefId  = doc.createElement("AttributeRefId")
    #     SamAttributeRefId.setAttribute("name","BioSample")
    #     SamRefId = doc.createElement("RefId")
    #     SamSPUID =  doc.createElement("SPUID")
    #     SamSPUID.setAttribute("spuid_namespace","NGDC")
    #     SamSPUID.appendChild(doc.createTextNode(CRR_data['sam_acc']+": "+CRR_data['sam_name']))
    #     SamRefId.appendChild(SamSPUID)
    #     SamAttributeRefId.appendChild(SamRefId)
    #     adddata.appendChild(SamAttributeRefId)
    # else:
    #     SamAttributeRefId=generate_known_biosample(SAMNacc)
    #     adddata.appendChild(SamAttributeRefId)

    Identifier = doc.createElement("Identifier")
    SPUID = doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(doc.createTextNode(
        CRR_data['run_acc']+": "+CRR_data['run_alias']))
    Identifier.appendChild(SPUID)
    adddata.appendChild(Identifier)

    return (action)


def generate_by_craacc(cra_acc):

    global doc
    global Submission
    doc = minidom.Document()

    Submission = doc.createElement('Submission')

    mysqlutils = mysqlUtils()

    # validate input CRA accession
    sql = "select * from cra WHERE accession = (%s) and cra.`status`='3' and cra.release_state ='2';"
    mysqlutils.excute(sql, cra_acc)
    check = mysqlutils.fetchOne()
    if check == None:
        logging.info(
            "CRA accession '{}' does not exist or is not released.".format(cra_acc))
        sys.exit()

    logging.info("processing the action: Description")
    project = mysqlutils.select_bioproject_by_cra_accession(cra_acc)
    user = mysqlutils.select_contact_by_cra_acc(cra_acc)
    Description = generate_description(cra_acc, project['release_time'].strftime(
        "%Y-%m-%d"), user["first_name"], user["last_name"], user["email"], user["organization"])
    Submission.appendChild(Description)

    PRJNAacc = sq.FetchAccession("PRJCAacc", project["accession"])  # 检查project

    if PRJNAacc == None or PRJNAacc["PRJNAacc"] == None or PRJNAacc["PRJNAacc"] == "":
        logging.info("processing the action: BioProject")
        project_sample_scope = mysqlutils.select_sample_scope_name_by_cra_accession(
            cra_acc)
        prj_data_type = mysqlutils.select_prj_data_type_by_cra_accession(
            cra_acc)
        prj_taxon_name = mysqlutils.select_taxon_name_by_cra_accession(cra_acc)

        prjaction1 = generate_bioproject(target_db="BioProject",
                                         prj_accn=project['accession'],
                                         prj_title=project['title'],
                                         prj_description=project['description'],
                                         prj_relevance=project['relevance'],
                                         prj_sample_scope=project_sample_scope,
                                         prj_data_type=prj_data_type,
                                         prj_taxon_name=prj_taxon_name
                                         )
        Submission.appendChild(prjaction1)
    else:
        # 生成已知编号的project
        pass

    logging.info("processing the action: BioSamples")
    samples_with_attrs = mysqlutils.select_samples_with_attrs_by_cra_accession(
        cra_acc)
    logging.debug("samples_with_attrs:{}".format(samples_with_attrs))
    sample_element_list = []
    for one_sample in samples_with_attrs:
        SAMNacc = sq.FetchAccession("SAMCacc", one_sample['accession'])
        # print(SAMNacc)

        if SAMNacc == None or SAMNacc["SAMNacc"] == None or SAMNacc["SAMNacc"] == "":
            sam_action1 = generate_biosample(
                target_db="BioSample",
                spuid=one_sample['accession'] + ": "+one_sample['name'],
                sample_package=one_sample['package'],
                sample_taxonname=one_sample['taxon_name'],

                sample_with_attr=one_sample
            )
            sample_element_list.append(sam_action1)
        else:
            pass

    for x in sample_element_list:
        Submission.appendChild(x)

    logging.info("processing the action: Runs")
    sample_exp_run = mysqlutils.select_biosampleAndExperimentAndRUN_by_cra_accession(
        cra_acc)
    logging.debug("sample_exp_run:{}".format(sample_exp_run))
    run_element_list = []
    for oneRun in sample_exp_run:
        RUN_action1 = generate_sra("SRA", project['accession'], oneRun)
        run_element_list.append(RUN_action1)
    for x in run_element_list:
        Submission.appendChild(x)

    doc.appendChild(Submission)

    fp = open(out, 'w')
    # doc.writexml(fp, indent='\t', addindent='\t', newl='\n', encoding="utf-8")
    # fp = open(f"{cra_acc}.xml", 'w')
    doc.writexml(fp, indent='\t', addindent='\t', newl='\n', encoding="utf-8")

    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="Convert GSA meta to xml with SRA format.")

    parser.add_argument(
        "--input", "-i", help="one CRA accession", required=True,)
    parser.add_argument(
        "-o", "--output", help="name of the output xml", required=True)
    args = parser.parse_args()

    generate_by_craacc(args.input, args.output)

# if __name__=="__main__":

    # parser = argparse.ArgumentParser(prog="Convert GSA meta to xml with SRA format.")

    # parser.add_argument("--input","-i",help="one CRA accession",required=True,)
    # parser.add_argument("-o","--output",help="name of the output xml",required=True)
    # args = parser.parse_args()
    # doc = minidom.Document()

    # Submission = doc.createElement('Submission')

    # mysqlutils  = mysqlUtils()

    # # validate input CRA accession
    # sql = "select * from cra WHERE accession = (%s) and cra.`status`='3' and cra.release_state ='2';"
    # mysqlutils.excute(sql,args.input)
    # check = mysqlutils.fetchOne()
    # if check == None:
    #     logging.info("CRA accession '{}' does not exist or is not released.".format(args.input))
    #     sys.exit()

    # logging.info("processing the action: Description")
    # project = mysqlutils.select_bioproject_by_cra_accession(args.input)
    # user=mysqlutils.select_contact_by_cra_acc(args.input)
    # Description = generate_description("CRA006605", project['release_time'].strftime("%Y-%m-%d"),user["first_name"],user["last_name"],user["email"],user["organization"])
    # Submission.appendChild(Description)

    # logging.info("processing the action: BioProject")
    # project_sample_scope =mysqlutils.select_sample_scope_name_by_cra_accession(args.input)
    # prj_data_type = mysqlutils.select_prj_data_type_by_cra_accession(args.input)
    # prj_taxon_name = mysqlutils.select_taxon_name_by_cra_accession(args.input)

    # prjaction1 = generate_bioproject(target_db="BioProject",
    # prj_accn=project['accession'],
    # prj_title=project['title'],
    # prj_description=project['description'],
    # prj_relevance=project['relevance'],
    # prj_sample_scope=project_sample_scope,
    # prj_data_type = prj_data_type,
    # prj_taxon_name=prj_taxon_name
    # )
    # Submission.appendChild(prjaction1)

    # logging.info("processing the action: BioSamples")
    # samples_with_attrs = mysqlutils.select_samples_with_attrs_by_cra_accession(args.input)
    # logging.debug("samples_with_attrs:{}".format(samples_with_attrs))
    # sample_element_list = []
    # for one_sample in samples_with_attrs:
    #     sam_action1= generate_biosample(
    #         target_db="BioSample",
    #         spuid= one_sample['accession'] + ": "+one_sample['name'] ,
    #         sample_package = one_sample['package'],
    #         sample_taxonname = one_sample['taxon_name'],

    #         sample_with_attr= one_sample
    #         )
    #     sample_element_list.append(sam_action1)

    # for x in sample_element_list:
    #     Submission.appendChild(x)

    # logging.info("processing the action: Runs")
    # sample_exp_run = mysqlutils.select_biosampleAndExperimentAndRUN_by_cra_accession(args.input)
    # logging.debug("sample_exp_run:{}".format(sample_exp_run))
    # run_element_list = []
    # for oneRun in sample_exp_run:
    #     RUN_action1 = generate_sra("SRA",project['accession'],oneRun)
    #     run_element_list.append(RUN_action1)
    # for x in run_element_list:
    #     Submission.appendChild(x)

    # doc.appendChild(Submission)

    # fp = open(args.output, 'w')
    # doc.writexml(fp, ndent='\t', addindent='\t', newl='\n', encoding="utf-8")
    # generate_by_craacc("CRA008976")
