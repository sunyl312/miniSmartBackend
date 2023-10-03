# from xml.dom import minidom
import logging
import sys
from GSA2xml import ComplexEncoder,mysqlUtils 
import GSA2xml
from xml.dom import minidom

mysqlutils  = mysqlUtils()
doc = minidom.Document()


def GSA2XMl(cra_acc):
    Submission = doc.createElement('Submission')
    # validate input CRA accession
    sql = "select * from cra WHERE accession = (%s) and cra.`status`='3' and cra.release_state ='2';"
    mysqlutils.excute(sql,cra_acc)
    check = mysqlutils.fetchOne()
    if check == None:
        logging.info("CRA accession '{}' does not exist or is not released.".format(cra_acc))
        sys.exit()



    logging.info("processing the action: Description")
    project = mysqlutils.select_bioproject_by_cra_accession(cra_acc)
    user=mysqlutils.select_contact_by_cra_acc(cra_acc)
    print(user)
    Description = GSA2xml.generate_description("CRA006605", project['release_time'].strftime("%Y-%m-%d"),user["first_name"],user["last_name"],user["email"],user["organization"])
    Submission.appendChild(Description)        

    logging.info("processing the action: BioProject")
    project_sample_scope =mysqlutils.select_sample_scope_name_by_cra_accession(cra_acc)
    prj_data_type = mysqlutils.select_prj_data_type_by_cra_accession(cra_acc)
    prj_taxon_name = mysqlutils.select_taxon_name_by_cra_accession(cra_acc)

    prjaction1 = mysqlutils.generate_bioproject(target_db="BioProject",
    prj_accn=project['accession'],
    prj_title=project['title'],
    prj_description=project['description'],
    prj_relevance=project['relevance'],
    prj_sample_scope=project_sample_scope,
    prj_data_type = prj_data_type,
    prj_taxon_name=prj_taxon_name
    )
    Submission.appendChild(prjaction1)



    logging.info("processing the action: BioSamples")    
    samples_with_attrs = mysqlutils.select_samples_with_attrs_by_cra_accession(cra_acc)
    logging.debug("samples_with_attrs:{}".format(samples_with_attrs))
    sample_element_list = []
    for one_sample in samples_with_attrs:
        sam_action1= mysqlutils.generate_biosample(
            target_db="BioSample",
            spuid= one_sample['accession'] + ": "+one_sample['name'] ,
            sample_package = one_sample['package'],
            sample_taxonname = one_sample['taxon_name'],
            sample_with_attr= one_sample
            )
        sample_element_list.append(sam_action1)


    for x in sample_element_list:
        Submission.appendChild(x)


    logging.info("processing the action: Runs")         
    sample_exp_run = mysqlutils.select_biosampleAndExperimentAndRUN_by_cra_accession(cra_acc)
    logging.debug("sample_exp_run:{}".format(sample_exp_run))
    run_element_list = []
    for oneRun in sample_exp_run:
        RUN_action1 = GSA2xml.generate_sra("SRA",project['accession'],oneRun)
        run_element_list.append(RUN_action1)
    for x in run_element_list:
        Submission.appendChild(x)

    doc.appendChild(Submission)
    
    out_name='{}_submission.xml'.format(cra_acc)

    fp = open("./GSA_XML/{}".format(out_name), 'w')
    doc.writexml(fp, indent='\t', addindent='\t', newl='\n', encoding="utf-8")

if __name__=="__main__":
    GSA2XMl("CRA008976")