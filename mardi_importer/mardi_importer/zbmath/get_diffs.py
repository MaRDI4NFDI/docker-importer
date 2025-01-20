import pandas as pd
import argparse


def compare_large_dfs(
    data_file_1,
    data_file_2,
    hash_file_1,
    hash_file_2,
    de_col="de_number",
    hash_col="hash",
    chunksize=50_000,
    output_file="differences.csv"
):
    """
    Compare two large data CSVs by de_number, based on
    hashes read from memory. For rows with different hashes,
    create a new output CSV that has all columns from df1,
    plus for each col, a col_new that shows the df2 value
    if it differs from df1, or empty otherwise.
    """
    conflict_string = "zbMATH Open Web Interface contents unavailable due to conflicting licenses"

    # -------------------------------------------------------------------------
    # 1. READ HASH FILES (SMALL ENOUGH TO FIT INTO MEMORY)
    # -------------------------------------------------------------------------
    print("Reading hash files into memory...")
    hash_df1 = pd.read_csv(hash_file_1, usecols=[de_col, hash_col], sep="\t")
    hash_df2 = pd.read_csv(hash_file_2, usecols=[de_col, hash_col], sep="\t")

    # Convert to dictionary: {de_number: hash_val}
    hash_dict1 = dict(zip(hash_df1[de_col], hash_df1[hash_col]))
    hash_dict2 = dict(zip(hash_df2[de_col], hash_df2[hash_col]))

    # Find de_numbers in both that differ
    common_de_numbers = set(hash_dict1.keys()).intersection(set(hash_dict2.keys()))
    diff_de_numbers = {de for de in common_de_numbers if hash_dict1[de] != hash_dict2[de]}

    if not diff_de_numbers:
        print("No differences found in hashes; nothing to compare.")
        return
    
    print(f"Found {len(diff_de_numbers)} de_numbers with different hashes.")

    # -------------------------------------------------------------------------
    # 2. PREPARE OUTPUT FILE
    # -------------------------------------------------------------------------
    # We'll write the result incrementally. First, create/clear the file.
    open(output_file, "w").close()

    first_write = True  # We only write the header on the first chunk

    # -------------------------------------------------------------------------
    # 3. CHUNK-READ DF1, THEN DF2, MERGE, AND WRITE DIFFERENCES
    # -------------------------------------------------------------------------
    df1_iter = pd.read_csv(data_file_1, chunksize=chunksize, sep="\t")
    col_names = None  # We'll get them from the first non-empty chunk

    

    for chunk1 in df1_iter:
        # Filter the chunk to only the differing de_numbers
        chunk1 = chunk1[chunk1[de_col].isin(diff_de_numbers)]
        if chunk1.empty:
            continue
        
        # Save column names from df1 so we know how to build new columns
        if col_names is None:
            col_names = chunk1.columns.tolist()

        # Figure out which de_numbers are in this chunk
        relevant_de_numbers = set(chunk1[de_col].unique())

        # Now read data_file_2 in chunks, filter by relevant_de_numbers, collect
        df2_iter = pd.read_csv(data_file_2, chunksize=chunksize, sep="\t")
        df2_for_this_chunk = []
        for chunk2 in df2_iter:
            chunk2_filtered = chunk2[chunk2[de_col].isin(relevant_de_numbers)]
            if not chunk2_filtered.empty:
                df2_for_this_chunk.append(chunk2_filtered)

        if not df2_for_this_chunk:
            # Means no rows from df2 matched. Continue to the next chunk1
            continue

        df2_filtered = pd.concat(df2_for_this_chunk, ignore_index=True)

        # Merge on de_number to compare side by side
        merged_chunk = pd.merge(
            chunk1,
            df2_filtered,
            on=de_col,
            how="inner",
            suffixes=("_df1", "_df2")
        )

        if merged_chunk.empty:
            continue

        # Build the output chunk:
        # We'll keep all columns from df1, but rename them back to original.
        # Then for each of those columns, create a <col>_new with the df2 value
        # if different, else empty.

        df1_cols = [c for c in merged_chunk.columns if c.endswith("_df1")]
        df2_cols = [c for c in merged_chunk.columns if c.endswith("_df2")]

        out_chunk = pd.DataFrame()

        # Bring over the df1 columns, renamed
        for c in df1_cols:
            original_col = c.replace("_df1", "")
            out_chunk[original_col] = merged_chunk[c]

        def no_conflict(val, conflict_string):
            """
            Returns True if:
            - val is NOT a string, or
            - val is a string but does NOT contain the conflict_string.
            """
            if isinstance(val, str):
                return conflict_string not in val
            else:
                # If it's not a string, treat it as if there's no conflict
                return True
                
        # Now create the new columns
        new_colnames = []
        for c_df1 in df1_cols:
            colname = c_df1.replace("_df1", "")
            c_df2 = colname + "_df2"
            new_colname = colname + "_new"
            new_colnames.append(new_colname)
            
            if c_df2 in df2_cols:
                # Fill with df2 value if different, else empty
                out_chunk[new_colname] = (
                    merged_chunk.apply(
                        lambda row: row[c_df2] if (row[c_df1] != row[c_df2]) and no_conflict(row[c_df2], conflict_string) else "",
                        axis=1
                    )
                )
            else:
                # If df2 doesn't have that column, just fill with empty
                out_chunk[new_colname] = ""

        empty_mask = out_chunk[new_colnames].eq('').all(axis=1)
        pruned_out_chunk = out_chunk[~empty_mask]
        if not pruned_out_chunk.empty:
            # Finally, write out the chunk
            if first_write:
                pruned_out_chunk.to_csv(output_file, index=False, mode="w",sep="\t")
                first_write = False
            else:
                pruned_out_chunk.to_csv(output_file, index=False, header=False, mode="a", sep="\t")

    print(f"Done! Differences written to '{output_file}'.")



parser = argparse.ArgumentParser()
parser.add_argument('--old_data_file')
parser.add_argument('--new_data_file')
parser.add_argument('--old_hash_file')
parser.add_argument('--new_data_file')

old_name = args.old_hash_file.split('/')[-1].split('-')[0]
new_name = args.new_hash_file.split('/')[-1].split('-')[0]

args = parser.parse_args()
compare_large_dfs(
        data_file_1=args.old_data_file,
        data_file_2=args.new_data_file',
        hash_file_1=args.old_hash_file,
        hash_file_2=args.new_hash_file,
        de_col="de_number",
        chunksize=50_000,
        output_file=f"differences_{old_name}_TO_{new_name}.csv"
    )
