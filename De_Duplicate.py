# This is a sample Python script.
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
# Gets the version
import snowflake.connector.pandas_tools

def Connection(usr,passwrd,acctName,wh,dbname,schema):
    try:
        ctx = snowflake.connector.connect(
            user=usr,
            password=passwrd,
            account=acctName,
            warehouse=wh,
            database=dbname,
            schema=schema
        )
        return ctx
    except snowflake.connector.errors.DatabaseError as db_error:
        print("There is an Error", db_error.errno)
        print(db_error.msg)


def Retreive(ctx):
    cs = ctx.cursor()
    try:
        cs.execute("select * from AUTHOR;")
        rows = 0
        while True:
            dat = cs.fetchall()
            if not dat:
                break
            df = pd.DataFrame(dat, columns=cs.description)
            rows += df.shape[0]
        if rows:
            for i in df.columns:
                print(i[0].ljust(10),end="\t")
            print()
            for i in df.values:
                for j in i:
                    if not j:
                        print("".ljust(10),end="\t")
                        continue
                    print(str(j).ljust(10),end="\t")
                print()
    except snowflake.connector.errors.ProgrammingError as db_error:
        print("There is an Error",db_error.errno)
        print(db_error.msg)
    finally:
        cs.close()


def Insert(ctx, fileName):
    cs = ctx.cursor()
    try:
        cs.execute("commit;")
        total = pd.read_csv(fileName, sep='^', header=0, index_col=False)
        total.reset_index(drop=True, inplace=True)
        for i in total.values:
            for j in i:
                print(j,end="\t")
            print()
        total = total.drop_duplicates(subset=['AUTHOR_UID']);
        print("\nAfter Dropping Duplicates-")
        for i in total.values:
            for j in i:
                print(j,end="\t")
            print()
        succcess, nchuncks, nrows, _ = write_pandas(ctx, total, "AUTHOR", quote_identifiers=False)
        print(str(succcess), str(nchuncks), str(nrows), sep="\t-")
        cs.execute("Create Table if NOT EXISTS Author_Dup as Select Distinct * from Author order by AUTHOR_UID;")
        cs.execute("Alter table Author_Dup SWAP WITH Author;")
        cs.execute("DROP TABLE IF EXISTS Author_Dup;")
    except snowflake.connector.errors.ProgrammingError as db_error:
        cs.execute("Rollback;")
        print("There is an Error", db_error.errno)
        print(db_error.msg)
    finally:
        cs.close()


ctx = Connection('KARANAG8','IamnottheUser123','va04221.southeast-asia.azure','COMPUTE_WH','LIBRARY_CARD_CATALOG','PUBLIC')
try:
    print("The data stored in a table-")
    Retreive(ctx)
    print("\nData in a File-")
    fileName = r"C:\Users\Karthik\Downloads\SRLeg_ASC\result.txt"
    Insert(ctx, fileName)
    print("\nTable After Insertion ----")
    Retreive(ctx)
finally:
    ctx.close()


