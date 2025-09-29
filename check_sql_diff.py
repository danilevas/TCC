import sys

def files_are_identical(file1, file2):
    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
        return f1.read() == f2.read()

if __name__ == "__main__":
    file1 = "_outros/SQLs/postgresql-caronae.sql"
    file2 = "_outros/SQLs/recriacao/banco_caronae_2019_10.sql"
    identical = files_are_identical(file1, file2)
    if identical:
        print("The files are identical.")
    else:
        print("The files are different.")