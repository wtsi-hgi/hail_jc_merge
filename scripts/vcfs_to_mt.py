import os
import hail as hl
import pandas as pd
import pyspark
import json
import sys
import re
from pathlib import Path
import logging
import argparse
from typing import Any, Counter, List, Optional, Tuple, Union


logging.basicConfig(format="%(levelname)s (%(name)s %(lineno)s): %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

project_root = Path(__file__).parent.parent
print(project_root)

s3credentials = os.path.join(
    project_root, "hail_configuration_files/s3_credentials.json")
print(s3credentials)

storage = os.path.join(project_root, "hail_configuration_files/storage.json")

thresholds = os.path.join(
    project_root, "hail_configuration_files/thresholds.json")

with open(f"{s3credentials}", 'r') as f:
    credentials = json.load(f)

with open(f"{storage}", 'r') as f:
    storage = json.load(f)

with open(f"{thresholds}", 'r') as f:
    thresholds = json.load(f)


################################
# Define the hail persistent storage directory
tmp_dir = "hdfs://spark-master:9820/"
temp_dir = "file:///home/ubuntu/data/tmp"
#lustre_dir = "file:///lustre/scratch123/teams/hgi/mercury/megaWES-variantqc"
plot_dir="/lustre/scratch123/teams/hgi/mercury/megaWES-variantqc"
lustre_dir = "file:///lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"
import_lustre_dir="file:/lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"
######################################

def import_vcfs_to_hail(path,vcf_header, prefix,suffix):
    objects = hl.utils.hadoop_ls(path)
    
    print("Reading vcf filenames")
    #vcfs = [vcf["path"] for vcf in objects if (vcf["path"].startswith(f"{s3location_input}chr"+chromosome+"_") and vcf["path"].endswith(".vcf.gz"))]
    vcfs = [vcf["path"] for vcf in objects if (vcf["path"].startswith(f"{path}/{prefix}") and vcf["path"].endswith(f"{suffix}"))]

    print(vcfs)

    mt = hl.import_vcf(vcfs, array_elements_required=False, 
                       force_bgz=True, header_file= vcf_header)
    print("Imported vcf files")
   
    return mt

   


def main():
    # Import UKB hits
    import_lustre_dir="file:/lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"
    vcf_header=f"{lustre_dir}/vcf_header.txt"
    prefix_files="ukbb"
    mt= import_vcfs_to_hail(import_lustre_dir,vcf_header,prefix_files,"vcf.gz")
    mt=hl.split_multi_hts(mt)
    mt.write(f"{lustre_dir}/matrixtables/ukbb_hits_split.mt", overwrite=True)

    # Import UKB AKT
    
    ukbb_vcf=f"{import_lustre_dir}/ukb_akt.sorted.vcf.gz"
    mt1 = hl.import_vcf(ukbb_vcf, reference_genome='GRCh38', force_bgz=True, array_elements_required=False)
    mt=hl.split_multi_hts(mt)
    mt.write(f"{lustre_dir}/matrixtables/ukb_akt_split.mt", overwrite=True)
    # Import UKB MAF > 0.01
    import_lustre_dir="file:/lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge/vcf_files/maf_001_ukb"
    vcf_header=f"{lustre_dir}/vcf_header.txt"
    prefix_files="ukb"
    mt= import_vcfs_to_hail(import_lustre_dir,vcf_header,prefix_files,"vcf.gz")
    mt=hl.split_multi_hts(mt)
    mt.write(f"{lustre_dir}/matrixtables/ukbb_MAF_0_01_split.mt")

    mt1=hl.read_matrix_table(f"{lustre_dir}/matrixtables/ukbb_hits_split.mt")
    mt2=hl.read_matrix_table(f"{lustre_dir}/matrixtables/ukb_akt_split.mt")
    mt3=hl.read_matrix_table(f"{lustre_dir}/matrixtables/ukbb_MAF_0_01_split.mt")
    all_datasets=[mt1,mt2,mt3]
    mt=hl.MatrixTable.union_rows(*all_datasets)
    mt_ukb_final=mt.checkpoint(f"{lustre_dir}/matrixtables/ukbb_hits_akt_MAD_split.mt",overwrite=True)


   



    logger.info("finished importing ukb vcfs and wrote mts. Starting IBD")
    

    #IBD
    #Import IBD hits
    import_lustre_dir="file:/lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"
    vcf_header=f"{lustre_dir}/ibd_header.txt"
    prefix_files="IBD"
    mt= import_vcfs_to_hail(import_lustre_dir,vcf_header,prefix_files,"vcf.gz")
    mt=hl.split_multi_hts(mt)
    mt.write(f"{lustre_dir}/matrixtables/ibd_hits_split.mt", overwrite=True)
    
    # IBD AKT 
    ibd_vcf=f"{import_lustre_dir}/ibd_akt.vcf.gz"
    mt1 = hl.import_vcf(ibd_vcf, reference_genome='GRCh38', force_bgz=True, array_elements_required=False)
    mt=hl.split_multi_hts(mt1)
    mt.write(f"{lustre_dir}/matrixtables/ibd_akt_split.mt", overwrite=True)

if __name__ == "__main__":
    # need to create spark cluster first before intiialising hail
    sc = pyspark.SparkContext()
    # Define the hail persistent storage directory

    hl.init(sc=sc, tmp_dir=lustre_dir, local_tmpdir=lustre_dir, default_reference="GRCh38")
    # s3 credentials required for user to access the datasets in farm flexible compute s3 environment
    # you may use your own here from your .s3fg file in your home directory
    hadoop_config = sc._jsc.hadoopConfiguration()

    hadoop_config.set("fs.s3a.access.key", credentials["mer"]["access_key"])
    hadoop_config.set("fs.s3a.secret.key", credentials["mer"]["secret_key"])
    
   
    main()
