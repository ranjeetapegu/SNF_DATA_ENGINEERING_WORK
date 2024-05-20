show databases; 
use role accountadmin

CREATE OR REPLACE PROCEDURE sproc_get_excel(excel_file string, sheet_name string, table_number int default 1, table_gap INT default 0 , rows_to_skip INT default 0 )
returns STRING
language python runtime_version = 3.8
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl','et_xmlfile')
handler = 'get_excel'
as
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import Variant,StringType,VariantType, IntegerType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import os

def get_excel(session: snowpark.Session, excel_file, sheet_name, table_number, table_gap, rows_to_skip):
    # Get file from stage
    filename = os.path.basename(excel_file)
    staged_file = session.file.get(excel_file, "/tmp")
    xls_full_path = f"/tmp/{filename}"

    skip_header_rows = rows_to_skip

    for x in range(table_number):
        xls_df = pd.read_excel(xls_full_path ,sheet_name=sheet_name, header=skip_header_rows)

        # Detect end of table (if there are multiple tables in a single sheet)
        try:
            range_end = xls_df[xls_df.isnull().all(axis=1) == True].index.tolist()[0]
        except: 
            range_end = len(xls_df)
        
        # Drop column if all values for the column are null
        xls_df = xls_df.dropna(axis=1, how='all')
        #print(xls_df.head())
        #xls_df =  xls_df.astype({"Kargo Approved Answers": str})
        
        # Create Snowpark dataframe
        
      
        skip_header_rows = skip_header_rows + len(xls_df) + table_gap + 1

    snf = session.createDataFrame(xls_df)

    # Get row count from dataframe
    pre_rowcount = len(xls_df)
   

    # Append records
    snf.write.mode('overwrite').save_as_table('FAKE_ACCOUNT_DATA')



    return_msg = f"SUCCESS. Loaded {pre_rowcount} rows."

    return return_msg


$$;

// how to call the stored proce

call sproc_get_excel('@stg_unstructured/Fake_Account_Dat.xlsx', 'Chart_Account', 1, 0, 0);


select * from FAKE_ACCOUNT_DATA ;

