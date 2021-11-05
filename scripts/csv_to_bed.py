import csv
import argparse

def csv_to_bed(csv_file, bed_file):
    with open(csv_file, "r") as csvfile:
        #9," 136,464,447 ",C,T,0.0259,
        reader = csv.reader(csvfile, delimiter=",")

        headers = next(reader)[:1]

        for row in reader:
            if row[0].startswith("#"):
                continue
            if row[0].startswith(" "):
                continue
          

            chromosome = row[0]
            start = row[1]
            start=start.replace(',','')
            if start !='':
                stop=int(start)
                start=int(start)-1
                ref = row[3]
                alt = row[4]
            

                print("Chromosome\tStart\tStop\tREF\tALT")
                print(f"chr{chromosome}\t{start}\t{stop}\n")
                with open(bed_file, 'a') as outfile:
                    outfile.write(f"chr{chromosome}\t{start}\t{stop}\n")


def main(args):
    csv_to_bed(args.csv_file, args.bed_file)

if __name__ == "__main__":
    
    parser= argparse.ArgumentParser()
    input_params = parser.add_argument_group("Imput params")
    output_params = parser.add_argument_group("Output_params")
    input_params.add_argument(
        "--csv_file",
        help="Full path of csv file",
        type=str,
    )
    output_params.add_argument(
        "--bed_file",
        help="Full path and name of output bed file",
        type=str,
    )

    args= parser.parse_args()
    main(args)
