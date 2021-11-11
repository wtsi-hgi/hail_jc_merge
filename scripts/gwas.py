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
from bokeh.plotting import output_file, save, show

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
plot_dir="lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"
lustre_dir = "file:///lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"
import_lustre_dir="file:/lustre/scratch123/mdt1/projects/wes_jc_ukb_ibd/hail_merge"


def main():
    # Do gwas
    
    logger.info("Read merged matrixtable and annotate")
    mt=hl.read_matrix_table(f"{lustre_dir}/matrixtables/IBD_UKBB_merged_split_final.mt")
    mt_annotated=mt.annotate_cols(cohort=(hl.case()
                .when(mt.s.startswith("EGAN"),"IBD")
                .default("UKBB")
                ))
    mt_annotated=mt_annotated.annotate_cols(
    pheno=
    (hl.case()
     .when(mt_annotated.cohort=="IBD",1)
     .when(mt_annotated.cohort=="UKBB",0)
     .default(0)
    )

)

    mt_ibd=mt_annotated.filter_cols(mt_annotated.cohort=="IBD")
    print(f"mt_ibd_count:{mt_ibd.count()}")
    

    mt=hl.variant_qc(mt_annotated)

    gwas = hl.linear_regression_rows(
        y=mt.pheno,
        x=mt.GT.n_alt_alleles(), covariates=[1.0], pass_through=[mt.rsid])

    gwas=gwas.checkpoint(f"{lustre_dir}/gwas_merged.table", overwrite=True)
    gwas.export(
            f"{lustre_dir}/gwas_merged.tsv.gz", header=True)

    p = hl.plot.manhattan(gwas.p_value, title=f" GWAS")
    output_file(
            f"{plot_dir}/gwas_merged_manhattan.html", mode='inline')
    save(p)
    print(f"Plotting QQ plot for ")
    q = hl.plot.qq(gwas.p_value, collect_all=False,
                       n_divisions=100, title=f" QQ plot")
    output_file(
            f"{plot_dir}/gwas/gwas_merged-QQplot.html", mode='inline')
    save(q)

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