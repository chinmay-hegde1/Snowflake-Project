# This is a sample Python script.
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
# Gets the version


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
        print("There is an Error -", db_error.errno)
        print(db_error.msg)


def Retreive(ctx):
    cs = ctx.cursor()
    try:
        cs.execute("select * from AUTHOR order by AUTHOR_UID;")
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
        cs.execute("Create temporary table Auth_2(AUTHOR_UID number(2,0), FIRST_NAME varchar(30) not null, "
                   "MIDDLE_NAME varchar(30), LAST_NAME varchar(30) not null)")
        total = pd.read_csv(fileName, sep='^', header=0, index_col=False)
        total.reset_index(drop=True, inplace=True)
        for i in total.values:
            for j in i:
                print(j,end="\t")
            print()
        total = total.drop_duplicates(subset=['AUTHOR_UID']);
        succcess, nchuncks, nrows, _ = write_pandas(ctx, total, "AUTH_2", quote_identifiers=False)
        print(str(succcess), str(nchuncks), str(nrows), sep="\t-")
        cs.execute("Merge into AUTHOR using AUTH_2 on Auth_2.Author_UID = Author.AUTHOR_UID "
                   "when matched then update set Author.FIRST_NAME = Auth_2.FIRST_NAME, "
                   "Author.MIDDLE_NAME = Auth_2.MIDDLE_NAME, Author.LAST_NAME = Auth_2.LAST_NAME "
                   "when not matched then "
                   "insert (AUTHOR_UID,FIRST_NAME,MIDDLE_NAME,LAST_NAME) "
                   "values (Auth_2.AUTHOR_UID,Auth_2.FIRST_NAME,Auth_2.MIDDLE_NAME,Auth_2.LAST_NAME);")
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
