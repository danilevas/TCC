import pandas as pd

def corrige(val):
    if isinstance(val, str):
        return val.replace("\r", "")

def row_values_diff(df1, df2, id, columns):
    # Check differences in column values for the line with id=id in df1 and df2
    row1 = df1[df1['id'] == id]
    row2 = df2[df2['id'] == id]

    if not row1.empty and not row2.empty:
        differences = {}
        for col in columns:
            val1 = row1.iloc[0][col]
            val2 = row2.iloc[0][col]

            val1 = corrige(val1)
            val2 = corrige(val2)

            if val1 != val2:
                differences[col] = (val1, val2)
        if differences:
            print(f"Differences for id={id}:")
            for col, (v1, v2) in differences.items():
                print(f"  {col}: file1={v1} | file2={v2}")
        else:
            print(f"No differences for id={id}.")
    else:
        print(f"id={id} not found in both files.")

def compare_csv(file1, file2, out_prefix="report", save_folder="_outros/csvs/diff"):
    # Read CSVs
    df1 = pd.read_csv(file1, sep=",")
    df2 = pd.read_csv(file2, sep=",")

    # Tratamento
    df1 = df1.map(corrige)
    df2 = df2.map(corrige)

    # Convert rows to sets of tuples for comparison
    set1 = set([tuple(row) for row in df1.to_numpy()])
    set2 = set([tuple(row) for row in df2.to_numpy()])

    # Differences
    only_in_file1 = set1 - set2
    only_in_file2 = set2 - set1
    in_both = set1 & set2

    columns = df1.columns.tolist() # assume both CSVs have same columns

    # Save to 3 dfs
    df1 = pd.DataFrame([dict(zip(columns, row)) for row in only_in_file1], columns=columns)
    df2 = pd.DataFrame([dict(zip(columns, row)) for row in only_in_file2], columns=columns)
    df_both = pd.DataFrame([dict(zip(columns, row)) for row in in_both], columns=columns)

    row_values_diff(df1, df2, 144118, columns)

    # Save to 3 separate files
    df1.to_csv(f"{save_folder}/{out_prefix}_only_in_file1.csv", sep=";", index=False)
    df2.to_csv(f"{save_folder}/{out_prefix}_only_in_file2.csv", sep=";", index=False)
    df_both.to_csv(f"{save_folder}/{out_prefix}_in_both.csv", sep=";", index=False)

    # Print counts
    print(f"✅ {len(only_in_file1)} rows only in {file1} → {out_prefix}_only_in_file1.csv")
    print(f"✅ {len(only_in_file2)} rows only in {file2} → {out_prefix}_only_in_file2.csv")
    print(f"✅ {len(in_both)} rows in both files → {out_prefix}_in_both.csv")

# Example usage:
file1 = r"C:\Users\PCRJ\Documents\GitHub\TCC\_outros\csvs\rides_2019_10.csv"
file2 = r"C:\Users\PCRJ\Documents\GitHub\TCC\_outros\csvs\rides_2020_03.csv"

compare_csv(file1, file2)