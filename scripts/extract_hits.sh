
declare bed="/lustre/scratch123/hgi/mdt1/projects/wes_jc_ukb_ibd/ibd_hotspots/cd2021_legit_hits.bed"

#declare IBD_FOFN=""
declare UKBB_FOFN="/lustre/scratch123/hgi/mdt1/projects/ukbiobank_genotypes/oct_2020_pvcf/vcf.fofn"
declare OUTPUT="/lustre/scratch123/hgi/projects/wes_jc_ukb_ibd/hail_merge"
#bcftools view -R cd2021_legit_hits.bed /lustre/scratch123/hgi/mdt1/projects/ukbiobank_genotypes/oct_2020_pvcf/ukb23156_cY_b0_v1.vcf.gz -O z -o test.vcf.gz
declare COUNT=0
#while read -r vcf; do COUNT=$(( $COUNT + 1 ));bsub -q long -G hgi -R'select[mem>10000] rusage[mem=10000]' -M10000  -o extract_${COUNT}.o -e extract_${COUNT}.e bcftools view -R $bed $vcf -O z -o ${OUTPUT}/ibd_${COUNT}.vcf.gz; done < "${IBD_FOFN}"
while read -r vcf; do COUNT=$(( $COUNT + 1 ));bsub -q long -G hgi -R'select[mem>10000] rusage[mem=10000]' -M10000  -o extract_${COUNT}.o -e extract_${COUNT}.e bcftools view -R $bed $vcf -O z -o ${OUTPUT}/ukbb_${COUNT}.vcf.gz; done < "${UKBB_FOFN}"

declare IBD_VCF="/lustre/scratch123/hgi/projects/wes_jc_ukb_ibd/glnexus_ibd/output.vcf.gz"
bsub -q long -G hgi -R'select[mem>10000] rusage[mem=10000]' -M10000  -o extract_IBD.o -e extract_IBD.e bcftools view -R $bed ${IBD_VCF} -O z -o ${OUTPUT}/IBD_hits.vcf.gz