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
        cs.execute("select * from AUTHOR_SCD order by END_DATE desc,AUTHOR_UID;")
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
        #cs.execute("Create temporary table if not exists Author_Temp_1(AUTHOR_UID number(2,0), "
                   #"FIRST_NAME varchar(30) not null, MIDDLE_NAME varchar(30), LAST_NAME varchar(30) not null,"
                   #"END_DATE DATE NOT NULL DEFAULT DATE('12/31/9999'));")
        cs.execute("Create temporary table if not exists Author_Temp_1(AUTHOR_UID number(2,0), "
                   "FIRST_NAME varchar(30) not null, MIDDLE_NAME varchar(30), LAST_NAME varchar(30) not null,"
                   "END_DATE DATE NOT NULL DEFAULT DATE('12/31/9999'));")
        #cs.execute("Insert into Author_Temp_2 Select * from AUTHOR_SCD2")
        total = pd.read_csv(fileName, sep=',', header=0, index_col=False)
        total.reset_index(drop=True, inplace=True)
        total = total.sort_values(['AUTHOR_UID', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME'])
        for i in total.values:
            for j in i:
                print(j,end="\t")
            print()
        total = total.drop_duplicates(subset=['AUTHOR_UID']);
        succcess, nchuncks, nrows, _ = write_pandas(ctx, total, "Author_Temp_1", quote_identifiers=False)
        print(str(succcess), str(nchuncks), str(nrows), sep="\t-")
        a=cs.execute("Merge into Author_SCD using Author_Temp_1 "
                   "on Author_Temp_1.Author_UID = Author_SCD.AUTHOR_UID "
                   "when matched AND AUTHOR_SCD.END_DATE = DATE('9999-12-31') AND"
                   "(Author_SCD.FIRST_NAME <> Author_Temp_1.FIRST_NAME "
                   "OR Author_SCD.MIDDLE_NAME <> Author_Temp_1.MIDDLE_NAME "
                   "OR Author_SCD.LAST_NAME <> Author_Temp_1.LAST_NAME)"
                   "then update set Author_SCD.END_DATE=current_date();")
        rows_update=a.rowcount
        a=cs.execute("Insert into AUTHOR_SCD Select * from Author_Temp_1 minus Select * from AUTHOR_SCD;")
        rows_insert=a.rowcount
        a=cs.execute("select * from AUTHOR_SCD;")
        number_of_rows=a.rowcount
        print(rows_update,rows_insert,number_of_rows)

        #succcess, nchuncks, nrows, _ = write_pandas(ctx, total, "Author_Temp_2", quote_identifiers=False)
        #print(str(succcess), str(nchuncks), str(nrows), sep="\t-")
        #cs.execute("Delete from AUTHOR_SCD2;")

        #cs.execute("Drop table if exists Author_Temp_1")
        #cs.execute("Drop table if exists Author_Temp_2")
        table_name='AUTHOR_SCD'
        db_name='SOCIAL_MEDIA_FLOODGATES'
        audit(ctx,db_name,table_name,number_of_rows,rows_update,rows_insert)
    except snowflake.connector.errors.ProgrammingError as db_error:
        cs.execute("Rollback;")
        print("There is an Error", db_error.errno)
        print(db_error.msg)
    finally:
        cs.close()
def audit(ctx,db_name,table_name,number_of_rows,rows_update,rows_insert):
    ctx.cursor().execute("create sequence if not exists Audit_Sequence start = 1 increment = 1;")
    ctx.cursor().execute("CREATE TABLE if not exists " "AUDIT_TABLE(AUDIT_ID NUMBER Default Audit_Sequence.nextval,load_dt DATE DEFAULT DATE(current_date()),db_name varchar(60),table_name varchar(60),rows_at_source number(3,0),rows_inserted number(3,0),rows_updated number(3,0));")
    ctx.cursor().execute("insert into AUDIT_TABLE VALUES(default,default,%s,%s,%s,%s,%s);",(db_name, table_name,number_of_rows, rows_insert, rows_update))

ctx = Connection('chinmay18','Chinmay1234','CI68775.canada-central.azure','SMALL_WH','SOCIAL_MEDIA_FLOODGATES','PUBLIC')
try:
    #ctx.cursor().execute("CREATE OR REPLACE TABLE " "AUTHOR_SCD(AUTHOR_UID varchar(60),FIRST_NAME varchar(60),MIDDLE_NAME varchar(60),LAST_NAME varchar(60), END_DATE DATE NOT NULL DEFAULT DATE('12/31/9999'))")
    print("The data stored in a table-")
    Retreive(ctx)
    print("\nData in a File-")
    fileName = r"C:\Users\chinm\chinmay\Author.csv"
    Insert(ctx, fileName)
    print("\nTable After Insertion ----")
    Retreive(ctx)
    a=ctx.cursor().execute("INSERT INTO TEST_TABLE VALUES('1','deeksha','g','bhat');")
    result1=a.rowcount
    a=ctx.cursor().execute("SELECT * FROM TEST_TABLE")
    result2=a.rowcount
    audit(ctx,'SOCIAL_MEDIA_FLOODGATES','TEST_TABLE',result2,0,result1)



finally:
    ctx.close()
