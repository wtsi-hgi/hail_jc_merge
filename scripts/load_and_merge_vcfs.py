from hail import Table
import os
import hail as hl
import pandas as pd
import numpy as np
import pyspark

#directory set up
tmp_dir = "hdfs://spark-master:9820/"
temp_dir = "file:///home/ubuntu/data/tmp"
lustre_dir = "file:///lustre/scratch123/mdt2/teams/hgi/mercury/ruth-test"
import_lustre_dir="file:/lustre/scratch123/mdt2/teams/hgi/mercury/IBD_UKBB_52k/VCFs"

def load_vcfs_to_hail_one_chrom(path,vcf_header, prefix,suffix):
    objects = hl.utils.hadoop_ls(path)
    print("Loading VCFs")
    vcfs = []
    vcf_add = [vcf["path"] for vcf in objects if (vcf["path"].startswith(f"{path}/{prefix}") and vcf["path"].endswith(f"{suffix}"))]
    vcfs = vcfs + vcf_add
    print(vcfs)
    print("Importing")
    #check the first file exists and has a size
    #print(os.path.getsize(vcfs[0]))
    mt = hl.import_vcf(vcfs, array_elements_required=False, force_bgz=True, header_file= vcf_header)
    print(mt.count())
    print("Imported")

    return mt

def split_multiallelic_from_file(mt_file, out_file):
    print("Splitting multiallelic sites from file: " + mt_file)
    mt=hl.read_matrix_table(mt_file)
    print(mt.count())
    mt=hl.split_multi_hts(mt)
    print(mt.count())
    print("Writing to outfile: " + out_file)
    mt.write(out_file, overwrite=True)

    return


def main():
    vcf_header=f"{lustre_dir}/vcf_header.txt"
    print(vcf_header)
    #print(os.path.getsize(vcf_header))#does header exist and have a size?
    mts_split = []
    for chrom in range(20,23):
        chromname = "chr" + str(chrom) + "_"
        mt = load_vcfs_to_hail_one_chrom(import_lustre_dir,vcf_header,chromname,"vcf.gz")
        mt_out_file = lustre_dir + "/matrixtables/chr" + str(chrom) + ".mt"
        print("Writing to file: " + mt_out_file)
        mt.write(mt_out_file, overwrite=True)
        
        #print("Writing to file: " + "{lustre_dir}/matrixtables/chr" + str(chrom) + ".mt")
        #mt.write(f"{lustre_dir}/matrixtables/chr" + str(chrom) + ".mt", overwrite=True)
        #reload matrix table
        #split multiallelic
        # mt_out_file_split = lustre_dir + "/matrixtables/chr" + str(chrom) + "_split.mt"
        #split_multiallelic_from_file(mt_out_file, mt_out_file_split)
        # mts_split.append(mt_out_file_split)
    #reload all 3 mts and merge rows into one mt, and save
    # print("Loading files to merge")
    # mt20file = lustre_dir + "/matrixtables/chr20_split.mt"
    # mt20 = hl.read_matrix_table(mt20file)
    # mt21file = lustre_dir + "/matrixtables/chr21_split.mt"
    # mt21 = hl.read_matrix_table(mt20file)
    # mt22file = lustre_dir + "/matrixtables/chr22_split.mt"
    # mt22 = hl.read_matrix_table(mt20file)
    # to_merge = [mt20, mt21, mt22]
    # print("Merging")
    # mt_merged = hl.MatrixTable.union_rows(*to_merge)
    # print("Merged")
    # print(mt_merged.count())
    # merge_file = lustre_dir + "/matrixtables/merged_20_21_22.mt"
    # mt_merged.write(merge_file, overwrite=True)
    # print("Saved merged matrixtable to file")



#hail init
if __name__ == "__main__":
    #init spark and hail
    sc = pyspark.SparkContext()
    hl.init(sc=sc, tmp_dir=lustre_dir, local_tmpdir=lustre_dir, default_reference="GRCh38")
    hadoop_config = sc._jsc.hadoopConfiguration()
    hadoop_config.set("fs.s3a.access.key", "")
    hadoop_config.set("fs.s3a.secret.key", "")
    hadoop_config.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_config.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    main()