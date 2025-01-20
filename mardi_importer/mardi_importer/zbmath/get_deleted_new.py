import csv
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--old_hash_file')
parser.add_argument('--old_hash_file')
parser.add_argument('--new_data_file')

args = parser.parse_args()

def read_de_numbers_csv(filename, de_column_name="de_number"):
    de_values = set()
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            de_values.add(row[de_column_name])
    return de_values

def subset_large_file_by_ids(
    input_file, 
    output_file, 
    de_values, 
    id_column="de_number", 
    chunksize=100000
):
    """
    Reads input_file in chunks, filters rows where id_column is in de_values,
    and writes them to output_file without loading the entire file into memory.
    """
    # You can tune chunksize depending on your memory constraints.
    first_chunk = True
    with pd.read_csv(input_file, sep='\t', chunksize=chunksize) as reader:
        for chunk in reader:
            # Filter this chunk
            chunk_filtered = chunk[chunk[id_column].isin(de_values)]
            
            # Write out the filtered rows
            chunk_filtered.to_csv(
                output_file,
                sep='\t',
                index=False,
                header=first_chunk,  # write header only for the first chunk
                mode='a'             # append to the file
            )
            first_chunk = False

old_name = args.old_hash_file.split('/')[-1].split('-')[0]
new_name = args.new_hash_file.split('/')[-1].split('-')[0]
old_de = read_de_numbers_csv(args.old_hash_file)
new_de = read_de_numbers_csv(args.new_hash_file)

old_ones_not_in_new = [int(x) for x in list(old_de - new_de)]
new_ones_not_in_old = [int(x) for x in list(new_de - old_de)]
with open(f'deleted_{old_name}_TO_{new_name}.tsv', 'a+') as f:
    f.write("de_number\n")
    for de in old_ones_not_in_new:
        f.write(str(de)+'\n')
subset_large_file_by_ids(
    input_file=args.new_data_file,
    output_file=f'new_{old_name}_TO_{new_name}.tsv',
    de_values=new_ones_not_in_old,
    id_column="de_number",
    chunksize=100000
)