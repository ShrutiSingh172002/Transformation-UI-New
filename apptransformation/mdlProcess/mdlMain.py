from .logger import Logger
import pandas as pd
from pyrfc import Connection
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import win32com.client as win32
import numpy as np
import os
import pythoncom
import multiprocessing
from datetime import datetime
from pyrfc import ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError

from .splitter import XmlSplitter
from .mdlMapping import *
from .mdlTransRule import *
from .mdlEnum import *
import zipfile

try:
    from importlib.metadata import version, PackageNotFoundError
    try:
        __version__ = version("pyrfc")
    except PackageNotFoundError:
        __version__ = "unknown"
except ImportError:
    __version__ = "unknown"

logger = Logger.get_logger()


def add_prefix_suffix(df, field_name, affix, position='LEFT'):
        
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False

        if position == 'LEFT':
            df[field_name] = affix + df[field_name].astype(str)
        else:
            df[field_name] = df[field_name].astype(str) + affix

    except (ValueError, TypeError) as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processsing ADDPREFIX Rule."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
    return dicResult

def zip_folder(folder_path, output_zip_path=None):
    # Initialize variables
    dicResult = {} #Function Result Dictionary

    try:
        dicResult['iserror'] = False
        if not os.path.isdir(folder_path):
            raise ValueError(f"Folder does not exist: {folder_path}")

        folder_path = os.path.abspath(folder_path)

        # Default zip path: same name as folder with .zip extension
        if output_zip_path is None:
            output_zip_path = folder_path.rstrip(os.sep) + ".zip"

        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    abs_path = os.path.join(root, file)
                    rel_path = os.path.relpath(abs_path, folder_path)  # Relative path inside zip
                    zipf.write(abs_path, rel_path)

        dicResult['value'] = output_zip_path
    except (ValueError, TypeError) as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processsing Zip File."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
    return dicResult



@log_execution_time
def get_sap_table_fields(connection_params,table_name):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False
        # Create SAP connection
        with Connection(**connection_params) as conn:
            # Call RFC_READ_TABLE to get field metadata
            result = conn.call('RFC_READ_TABLE', QUERY_TABLE=table_name, ROWCOUNT=1)
            
            # Extract field names
            field_names = [field['FIELDNAME'] for field in result['FIELDS']]
            print(field_names)

        dicResult['value'] = field_names

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in getting table fields."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
    return dicResult

# Function to create an SQLAlchemy connection
@log_execution_time
def get_sqlalchemy_connection(server, database, username, password):

    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False
        # Define your SQLAlchemy connection string
        driver = 'ODBC Driver 17 for SQL Server'

        # Encode username and password
        encoded_username = quote_plus(username)
        encoded_password = quote_plus(password)
        connection_string = (
            f"mssql+pyodbc://{encoded_username}:{encoded_password}@{server}/{database}"
            f"?driver={quote_plus(driver)}"
        )
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in genretaing get_sqlalchemy_connection." + str(e)
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)

    return dicResult

@log_execution_time
def thread_extract_data_from_sap(connection_params,dictECCFieldMapping,sqltblname):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    dictDatadf = {}
    try:
        dicResult['iserror'] = False
        with ThreadPoolExecutor(max_workers=3) as executor:
            lsttask = []
            for tbl,list in dictECCFieldMapping.items():
                lsttask.append(executor.submit(get_data_from_sap_table_1,connection_params,tbl,list))

            for task in lsttask:
                dict = task.result()
                if dict['iserror'] == True:
                    dicResult['iserror'] = True
                    dicResult['error'] = dict['error']
                    dicResult['error_details'] = dict['error_details']
                    return dicResult
                dictDatadf.update(dict['value'])

        dicResult['value'] = dictDatadf

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in genretaing Dashboard Data."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)

    return dicResult

def get_data_from_sap_table(connection_params,tbl,lstFields):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    chunk_size = 1000
    try:
        dicResult['iserror'] = False
        # Establish SAP connection
        with Connection(**connection_params) as connection:
            logger.info(f"Connected to SAP successfully. for table Process: {tbl}")
            
            lstActField = []
            result = connection.call('RFC_READ_TABLE', QUERY_TABLE=tbl,ROWCOUNT=0)
            for field in result['FIELDS']:
                lstActField.append(field['FIELDNAME'])

            lstNotFoundField = [fld  for fld in lstFields if fld not in lstActField]
            if len(lstNotFoundField) > 0:
                strNotFoundFiled = ",".join(lstNotFoundField)
                strError = f"fields: {strNotFoundFiled} Not Found in table: {tbl}."
                dicResult['iserror'] = True
                dicResult['error'] = strError
                dicResult['error_details'] = strError
                logger.warning(strError)
                return dicResult
            
            # Chunked data fetch
            all_data = []
            skip_rows = 0

            while True:

                result = connection.call('RFC_READ_TABLE',DELIMITER = '|',QUERY_TABLE=tbl,FIELDS = lstFields,ROWSKIPS=skip_rows,ROWCOUNT=chunk_size)
                if not result or 'DATA' not in result:
                    break
                
                data_rows = [row['WA'] for row in result['DATA']]
                all_data.extend(data_rows)

                #logger.info(f"For Table: {tbl} Process. Fetched {len(data_rows)} rows (Skipped: {skip_rows}).")

                if len(data_rows) < chunk_size:
                    break  # Last chunk reached

                skip_rows += chunk_size

            if not all_data:
                strError = f"No data retrieved from SAP. for table {tbl}"
                dicResult['iserror'] = True
                dicResult['error'] = strError
                dicResult['error_details'] = strError
                logger.warning(strError)
                return dicResult
            
            # Extract columns
            columns = [field['FIELDNAME'] for field in result['FIELDS']]
            df = pd.DataFrame([row.split('|') for row in all_data], columns=columns)
            logger.info(f"Total {len(df)} rows fetched from table {tbl}.")
            dicResult['value'] = {tbl:df}

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = f"Unhandled Error in getting data from sap table: {tbl}."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
        logger.warning(dicResult['error_details'])

    return dicResult

def get_data_from_sap_table_1(connection_params, tbl, lstFields):
    dicResult = {}
    row_chunk_size = 1000  # Rows per chunk
    field_chunk_start_size = 10  # Initial number of fields per chunk

    try:
        dicResult['iserror'] = False
        all_data_df = pd.DataFrame()

        with Connection(**connection_params) as connection:
            logger.info(f"Connected to SAP successfully. Processing table: {tbl}")

            # Get actual available fields from the table
            result = connection.call('DDIF_FIELDINFO_GET', TABNAME=tbl, LANGU='EN', ALL_TYPES='X')
            actual_fields = list(set(field['FIELDNAME'] for field in result['DFIES_TAB']))

            # Validate fields
            lstNotFoundField = [fld for fld in lstFields if fld not in actual_fields]
            if lstNotFoundField:
                strError = f"Fields not found in table {tbl}: {','.join(lstNotFoundField)}"
                dicResult.update({'iserror': True, 'error': strError, 'error_details': strError})
                logger.warning(strError)
                return dicResult

            # Begin chunking by fields
            i = 0
            while i < len(lstFields):
                current_chunk_size = field_chunk_start_size
                success = False

                while not success and current_chunk_size > 0:
                    current_fields = lstFields[i:i+current_chunk_size]
                    temp_data = []
                    skip_rows = 0

                    try:
                        while True:
                            result = connection.call(
                                'RFC_READ_TABLE',
                                DELIMITER='|',
                                QUERY_TABLE=tbl,
                                FIELDS=[{'FIELDNAME': fld} for fld in current_fields],
                                ROWSKIPS=skip_rows,
                                ROWCOUNT=row_chunk_size
                            )

                            if not result or 'DATA' not in result or not result['DATA']:
                                break

                            temp_data.extend([row['WA'] for row in result['DATA']])
                            if len(result['DATA']) < row_chunk_size:
                                break
                            skip_rows += row_chunk_size

                        # Convert chunk data to DataFrame
                        df_chunk = pd.DataFrame(
                            [row.split('|') for row in temp_data],
                            columns=[f['FIELDNAME'] for f in result['FIELDS']]
                        )

                        # Concatenate column-wise
                        all_data_df = pd.concat([all_data_df, df_chunk], axis=1)
                        logger.info(f"Successfully fetched fields {i}-{i+current_chunk_size} from table {tbl}")
                        success = True
                        i += current_chunk_size

                    except (ABAPApplicationError, ABAPRuntimeError, CommunicationError, LogonError) as e:
                        logger.warning(f"Chunk fetch error at fields {i}-{i+current_chunk_size}: {e}")
                        current_chunk_size -= 2  # Reduce field chunk size and retry

                if current_chunk_size == 0:
                    logger.warning(f"Skipping field '{lstFields[i]}' due to repeated fetch failure.")
                    i += 1  # Skip the problematic field

            if all_data_df.empty:
                strError = f"No data retrieved from SAP for table {tbl}"
                dicResult.update({'iserror': True, 'error': strError, 'error_details': strError})
                logger.warning(strError)
                return dicResult

            dicResult['value'] = {tbl: all_data_df}
            logger.info(f"Total {len(all_data_df)} rows and {len(all_data_df.columns)} columns fetched from table {tbl}.")

    except Exception as e:
        strError = f"Unhandled error while fetching data from SAP table: {tbl} | {str(e)}"
        dicResult.update({'iserror': True, 'error': strError, 'error_details': strError})
        logger.warning(strError)

    return dicResult

@log_execution_time
def connect_to_sap(strTbl, lstFields = '',lngrowcnt = '', lngrowskps = ''):
    # SAP connection parameters
    connection_params = {
        'user': 'SANDIP',
        'passwd': 'Welcome@123456789',
        'ashost': '11.11.11.13',
        'sysnr': '00',
        'client': '100',
        'lang': 'EN',
        'trace': '0'  # Disable RFC logging
    }
    
    try:
        # Establish SAP connection
        with Connection(**connection_params) as connection:
            logger.info("Connected to SAP successfully.")
            
            lstActField = []
            result = connection.call('RFC_READ_TABLE', QUERY_TABLE=strTbl)
            for field in result['FIELDS']:
                lstActField.append(field['FIELDNAME'])


            # lstNotFoundField = [fld  for fld in lstFields if fld not in lstActField]
            # if len(lstNotFoundField) > 0:
            #     strNotFoundFiled = ",".join(lstNotFoundField)
            #     logger.warning(f"fields: {strNotFoundFiled} Not Found in table: {strTbl}.")
            #     return pd.DataFrame()


            # Fetch data from SKA1 table
            # result = connection.call('RFC_READ_TABLE',DELIMITER = '|',QUERY_TABLE=strTbl,FIELDS = lstFields,ROWSKIPS = lngrowskps , ROWCOUNT = lngrowcnt)
            
            result = connection.call('RFC_READ_TABLE',DELIMITER = '|',QUERY_TABLE=strTbl)
            #result = connection.call('RFC_READ_TABLE',DELIMITER = '|',QUERY_TABLE=strTbl,FIELDS = lstFields)
            if not result or 'DATA' not in result:
                logger.warning("No data retrieved from SAP.")
                return pd.DataFrame()
            
            # Extract column names
            columns = [field['FIELDNAME'] for field in result['FIELDS']]
            
            # Extract data rows
            data_rows = [row['WA'] for row in result['DATA']]
            
            # # Split each row by the delimiter '|'
            # split_data = [row.split('|') for row in data_rows]

            # Convert to DataFrame
            df = pd.DataFrame([row.split('|') for row in data_rows], columns=columns)
    
            df.to_excel('Data_Test.xlsx',index=False)

            logger.warning(f"{len(df)} Data Avilable Table.")
            
            logger.info("Data successfully retrieved and converted to DataFrame.")
            
            return df
    
    except Exception as e:
        logger.error(f"SAP Connection Error: {e}")
        return pd.DataFrame()
    
@log_execution_time
def fetch_ecc_mapping(server, database, username, password, templateid,clientid):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False
        # Create database connection
        engine = get_sqlalchemy_connection(server, database, username, password)


        if isinstance(engine, dict):
            dicResult['iserror'] = True
            dicResult['error'] = engine['error']
            dicResult['error_details'] = dicResult['error']
            return dicResult
        else:
            logger.info('SQL Connection Establish for to featch ECC Mapping')
        
        # Define the query
        query = text("""SELECT [SoruceTable],[SoruceField],[TargetTable],[TargetField]
                                ,[IsMainTable],[SoruceJoinFiled],[TargetJoinField]
                        FROM [ECC_Field_Mapping] WHERE [TemplateID] = :templateid AND (([ClientFlag] = '' OR [ClientFlag] IS NULL OR [ClientFlag] = :clientid))
                        AND LOWER(TRIM(:clientid)) NOT IN (SELECT LOWER(TRIM(value)) FROM STRING_SPLIT([ExcludeClientFlag], '|'))
                     """)

        params = {"templateid": templateid,'clientid':clientid}
        # Execute query
        with engine.connect() as conn:
            df  = pd.read_sql(query, conn, params=params)

        # Process data into dictionary
        ecc_dict = defaultdict(list)
        for index, row in df .iterrows():
            if row['SoruceField'] not in ecc_dict[row['SoruceTable']]:
                ecc_dict[row['SoruceTable']].append(row['SoruceField'])

        dicResult['value'] = ecc_dict
        dicResult['mapping_df'] = df

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in Fetching ECC Mapping."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)

    return dicResult

def fetch_tranformation_rule(server, database, username, password, clientid):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False
        # Establish database connection
        engine = get_sqlalchemy_connection(server, database, username, password)
        

        if isinstance(engine, dict):
            dicResult['iserror'] = True
            dicResult['error'] = engine['error']
            dicResult['error_details'] = dicResult['error']
            return dicResult
        else:
            logger.info('SQL Connection Establish for to featch Transformation Rule')

        # Define and execute query with parameterized input
        query = text("""SELECT [ClientID]
                            ,[TargetTable]
                            ,[TargetField]
                            ,[RuleName]
                            ,[Format]
                            ,[Custome1]
                            ,[Custome2]
                            ,[Custome3]
                        FROM [INNOVAPTE].[dbo].[ConditionalRules] WHERE [ClientID] = :clientid""")

        params = {"clientid": clientid}

        # Execute query
        with engine.connect() as conn:
            # Read results directly into DataFrame
            df  = pd.read_sql(query, conn, params=params)

        # Process rules into structured dictionary
        rules_dict = defaultdict(lambda: defaultdict(list))
        
        for _, row in df.iterrows():
            table = row['TargetTable']
            field = row['TargetField']
            
            # Split all pipe-delimited fields
            rule_names = str(row['RuleName']).split('|') if pd.notna(row['RuleName']) else ['']
            formats = str(row['Format']).split('|') if pd.notna(row['Format']) else ['']
            custom1 = str(row['Custome1']).split('|') if pd.notna(row['Custome1']) else ['']
            custom2 = str(row['Custome2']).split('|') if pd.notna(row['Custome2']) else ['']
            custom3 = str(row['Custome3']).split('|') if pd.notna(row['Custome3']) else ['']
            
            # Create rule info for each rule name
            for i, rule_name in enumerate(rule_names):
                if not rule_name.strip():
                    continue
                    
                rule_info = {
                    'rule_name': rule_name.strip(),
                    'format': [f.strip() for f in formats[i:i+1] or ['']],
                    'custom1': [c.strip() for c in custom1[i:i+1] or ['']],
                    'custom2': [c.strip() for c in custom2[i:i+1] or ['']],
                    'custom3': [c.strip() for c in custom3[i:i+1] or ['']]
                }
                
                # Ensure all lists have same length as rule_names
                rules_dict[table][field].append(rule_info)

        # Convert defaultdict to regular dict
        dicResult['value'] = {k: dict(v) for k, v in rules_dict.items()}

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in Fetching Transformation Rule."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)

    return dicResult

@log_execution_time
def fetch_transformation_details(server, database, username, password, saptemversion,templatename):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False
        # Create database connection
        engine = get_sqlalchemy_connection(server, database, username, password)

        if isinstance(engine, dict):
            dicResult['iserror'] = True
            dicResult['error'] = engine['error']
            dicResult['error_details'] = dicResult['error']
            return dicResult
        else:
            logger.info('SQL Connection Establish for to featch Transformation Details')
        
        # Define the query
        query = text("""SELECT [TemplateID] 
                        ,[LTMCVersion]
                        ,[TemplateName]
                        ,[BlankTemplatePath]
                        ,[Script]
                        ,[Script1]
                        ,[Script2]
                        ,[Script3]
                    FROM [tblTransformationMaster] 
            WHERE [LTMCVersion] = :saptemversion AND [TemplateName] = :templatename""")

        # Execute query
        with engine.connect() as conn:
            result = conn.execute(query, {"saptemversion": saptemversion, "templatename": templatename})
            rows = result.fetchone()

        if rows is None:
            dicResult['iserror'] = True
            dicResult['error'] = f"No Transformation Details available for {templatename}-{saptemversion}."
            dicResult['error_details'] = f"No Transformation Details available for {templatename}-{saptemversion}."

        dicResult['value'] = rows

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in Featching Transformation Details."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)

    return dicResult

@log_execution_time
def processs_transformation_rule(dictTransformationRule,dicttblmapping):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    dictRule = {}
    try:
        dicResult['iserror'] = False

        # Accessing the rules:
        for table, fields in dictTransformationRule.items():
            if table not in dicttblmapping:
                continue  # Skip if table doesn't exist in our data
            targetdf = dicttblmapping[table]

            for field, field_rules in fields.items():
                if field not in targetdf.columns:
                    continue  # Skip if field doesn't exist in this table
                
                for rule in field_rules:
                    rule_name = str(rule['rule_name'])
                    format_options = rule['format'][0] if rule['format'] and len(rule['format']) > 0 else ''
                    custom1_options = rule['custom1'][0] if rule['custom1'] and len(rule['custom1']) > 0 else ''
                    try:
                        if rule_name == 'ZEROFILL' and format_options:

                            dictRule = zero_pad_field(targetdf,field,format_options)

                            if dictRule['iserror'] == True:
                                dicResult['iserror'] = True
                                dicResult['error'] = dictRule['error']
                                dicResult['error_details'] = dictRule['error_details']
                                return dicResult

                        elif  rule_name == 'ADDPREFIX' and format_options and custom1_options:

                            dictRule = add_prefix_suffix(targetdf,field,format_options,custom1_options)

                            if dictRule['iserror'] == True:
                                dicResult['iserror'] = True
                                dicResult['error'] = dictRule['error']
                                dicResult['error_details'] = dictRule['error_details']
                                return dicResult

                    except Exception as e:
                        dicResult['iserror'] = True
                        dicResult['error'] = f"Error in Processing {rule_name} Transformation Rule."
                        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
                        return dicResult

        dicResult['value']  = dicttblmapping

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in Processing Transformation Rule."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)

    return dicResult

@log_execution_time
def table_mapping(dfeccmapping, source_data):
    dicResult = {}
    final_target_dataframes = {}
    try:
        dicResult['iserror'] = False
        logger.info("Starting table_mapping function.")

        dfmapping_main = dfeccmapping[['SoruceTable', 'SoruceField', 'TargetTable', 'TargetField', 'SoruceJoinFiled', 'TargetJoinField', 'IsMainTable']]
        target_groups = dfmapping_main.groupby('TargetTable')

        for target_table, group in target_groups:
            logger.info(f"Processing TargetTable: {target_table}")

            mapped_columns = {}
            main_rows = group[group['IsMainTable'] == 1]
            non_main_rows = group[group['IsMainTable'] == 0]

            # Estimate row count for alignment
            row_count = max([len(source_data[table]) for table in main_rows['SoruceTable'].unique() if table in source_data], default=0)

            # Process main table mappings
            for _, row in main_rows.iterrows():
                source_table = row['SoruceTable']
                source_field = row['SoruceField']
                target_field = row['TargetField']

                source_df = source_data.get(source_table)
                if source_df is not None and source_field in source_df.columns:
                    mapped_columns[target_field] = source_df[source_field].reset_index(drop=True)
                    #logger.debug(f"Mapped {source_table}.{source_field} to {target_table}.{target_field}")
                else:
                    mapped_columns[target_field] = pd.Series([pd.NA] * row_count)
                    logger.warning(f"Missing {source_table}.{source_field}, filled NaN for {target_table}.{target_field}")

            # Create DataFrame from mapped columns
            target_df = pd.DataFrame(mapped_columns)

            # Process non-main mappings with joins
            for _, row in non_main_rows.iterrows():
                source_table = row['SoruceTable']
                source_field = row['SoruceField']
                target_field = row['TargetField']

                source_df = source_data.get(source_table)
                if source_df is None:
                    logger.warning(f"Source table {source_table} not found, skipping join for {target_field}")
                    continue

                source_join_cols = str(row['SoruceJoinFiled']).split('|')
                target_join_cols = str(row['TargetJoinField']).split('|')

                join_fields_source = source_join_cols + [source_field]
                join_fields_target = target_join_cols + [target_field]

                if join_fields_source != join_fields_target:
                    col_mapping = dict(zip(join_fields_source, join_fields_target))
                    temp_df = source_df[join_fields_source].rename(columns=col_mapping).copy()
                    logger.debug(f"Renamed join columns: {col_mapping}")
                else:
                    temp_df = source_df[join_fields_source].copy()

                # Merge only if the target field not present
                if target_field not in target_df.columns:
                    target_df = pd.merge(target_df, temp_df, how='left', on=target_join_cols)
                    logger.debug(f"Merged {source_table} into {target_table} on {target_join_cols}")

            final_target_dataframes[target_table] = target_df
            logger.info(f"Completed mapping for TargetTable: {target_table}")

        dicResult['value'] = final_target_dataframes
        logger.info("table_mapping function completed successfully.")

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in ECC Table Mapping."
        dicResult['error_details'] = f"{dicResult['error']} | {str(e)}"
        logger.exception(f"Error in table_mapping: {e}")

    return dicResult

@log_execution_time
def write_multiple_sheets_to_excel(df_dict, template_path, save_path, batch_size=1000):

    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False

        try:
            logger.info("Starting write_multiple_sheets_to_excel...")

            # COM Initialization
            pythoncom.CoInitialize()
            excel_app = win32.gencache.EnsureDispatch('Excel.Application')
            excel_app.Visible = False
            excel_app.DisplayAlerts = False

            # Open Template Workbook
            wb = excel_app.Workbooks.Open(template_path)

            for sheet_name, df in df_dict.items():
                logger.info(f"Processing sheet: {sheet_name}")
                df.to_excel(f'{sheet_name}.xlsx',index =False)
                try:
                    ws = wb.Sheets(sheet_name)
                except Exception as e:
                    logger.warning(f"Sheet '{sheet_name}' not found. Skipping. Error: {e}")
                    continue

                ws.Unprotect()


                # Detect last used column using Excel method
                last_col = ws.Cells(5, ws.Columns.Count).End(-4159).Column  # -4159 is xlToLeft
                header_range = ws.Range(ws.Cells(5, 1), ws.Cells(5, last_col))
                template_headers = [cell.Value for cell in header_range if cell.Value is not None]

                # # Match DataFrame columns to template headers
                # matched_columns = [col for col in template_headers if col in df.columns]
                # if not matched_columns:
                #     logger.warning(f"No matching columns found in sheet '{sheet_name}'. Skipping.")
                #     continue

                # df_matched = df.reindex(columns=matched_columns, fill_value="")

                # Ensure all template headers exist in DataFrame; insert empty columns for missing ones
                df_aligned = pd.DataFrame(columns=template_headers)  # Create a new DataFrame with template headers

                for col in template_headers:
                    if col in df.columns:
                        df_aligned[col] = df[col]
                    else:
                        logger.warning(f"Column '{col}' from template is missing in data for sheet '{sheet_name}'. Filling with empty values.")
                        df_aligned[col] = ""  # Fill with empty string for missing columns

                # Fill NaNs to ensure clean Excel export
                df_aligned = df_aligned.fillna("")


                # Convert DataFrame to NumPy array for fast writing
                data_array = df_aligned.to_numpy(dtype=object)  # Ensure mixed data types are handled
                num_rows, num_cols = data_array.shape

                # Determine Excel write range
                start_row = 9
                total_batches = (num_rows // batch_size) + 1

                # Process in batches
                for batch_idx, i in enumerate(range(0, num_rows, batch_size), start=1):
                    batch = data_array[i:i + batch_size]
                    end_row = start_row + len(batch) - 1
                    rng = ws.Range(ws.Cells(start_row, 1), ws.Cells(end_row, num_cols))
                    if not rng.MergeCells:
                        rng.NumberFormat = "@"
                    # rng.NumberFormat = "General"
                    rng.Value = batch
                    start_row = end_row + 1

                    # if batch_idx % 10 == 0 or batch_idx == total_batches:
                    #     logger.info(f"Sheet '{sheet_name}': {batch_idx}/{total_batches} batches written.")

                logger.info(f"Finished writing {num_rows} rows to sheet '{sheet_name}'.")

            # Save Workbook
            wb.SaveAs(save_path)

            logger.info(f"All sheets written successfully. File saved at: {save_path}")

        except Exception as e:
            dicResult['iserror'] = True
            dicResult['error'] = "Error while writing template."
            dicResult['error_details'] = dicResult['error']  + " | " + str(e)
            logger.exception(dicResult['error_details'])
        finally:
            # Cleanup to release COM objects
            try:
                wb.Close(False)  # Close Workbook
            except:
                logger.warning("Workbook already closed or disconnected.")
            
            try:
                excel_app.Quit()  # Quit Excel
            except:
                logger.warning("Excel application was already closed.")

            pythoncom.CoUninitialize()

            logger.info('Successfully Cleanup in writing template')

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error while writing template."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
        logger.exception(dicResult['error_details'])

    return dicResult

def convert_bytes(size):
    """ Convert bytes to KB, or MB or GB"""
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return "%3.1f %s" % (size, x)
        size /= 1024.0

@log_execution_time
def XMLSplitter(XMLPath):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        logger.info("Entered into XMLSplitter Fucnction for Split")
        dicResult['iserror'] = False
        strSize =os.path.getsize(XMLPath)                                       
        IsKBMB=convert_bytes(strSize)
        logger.info("File Size: " + IsKBMB) 
        intNoOfFile = 1
        uppercase_name = XMLPath.upper()
        logger.info("XML File Path: " + uppercase_name) 
        if os.path.isfile(uppercase_name) is not True:
            strMsg = "XML File does not exist."
            dicResult['iserror'] = True
            dicResult['error'] = strMsg
            dicResult['error_details'] = strMsg
            logger.error(strMsg) 
            return dicResult
        elif uppercase_name.find(".XML") == -1:
            strMsg = "Provided file does not XML File."
            dicResult['iserror'] = True
            dicResult['error'] = strMsg
            dicResult['error_details'] = strMsg
            logger.error(strMsg) 
            return dicResult
        else:
            if IsKBMB.split()[1]=="MB":
                if float(IsKBMB.split()[0] )>=100:        
                    intNoOfFile = int(float(IsKBMB.split()[0] )/100)
                    if intNoOfFile ==1:
                        intNoOfFile=2
            elif IsKBMB.split()[1]=="GB":
                megaByte = float(IsKBMB.split()[0] )*1000
                intNoOfFile = int(megaByte/100)

            logger.info("Number of File: " + str(intNoOfFile))
            if intNoOfFile >1:
                logger.info("File more than 1 count")
                try:
                    strMainFile = XMLPath.split('.')[0]
                    for cntr in range(1,intNoOfFile+1):
                        strFinalFileName = strMainFile+"_("+ str(cntr) +")"+".xml"
                        if os.path.isfile(strFinalFileName) is True:
                            os.remove(strFinalFileName)
                    strFinalFileName = strMainFile+ "_invalid_data"+".xml"
                    if os.path.isfile(strFinalFileName) is True:
                        os.remove(strFinalFileName)
                except Exception as error:
                    strMsg = "Error in XML OutPut File Validation." + "[#]"+ str(error)
                    dicResult['iserror'] = True
                    dicResult['error'] = strMsg
                    dicResult['error_details'] = strMsg
                    logger.error(strMsg)
                    return dicResult
                else:
                    try:
                        splitter = XmlSplitter(XMLPath, intNoOfFile)
                        splitter.split()
                    except Exception as error:
                        strMsg = "Error in XML Split Main Process (Exe)." + "[#]" + str(error)
                        dicResult['iserror'] = True
                        dicResult['error'] = strMsg
                        dicResult['error_details'] = strMsg
                        logger.error(strMsg)
                        return dicResult
                    else:
                        os.remove(XMLPath)
                    
        dicResult['FileCnt'] = intNoOfFile

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in XML Split."
        dicResult['error_details'] = dicResult['error']  + "[#]" + str(e)
        logger.error(dicResult['error_details'])
    return dicResult

if __name__ == '__main__':
    multiprocessing.freeze_support()

@log_execution_time
def process_transformation(server,database,username,password,saptepmversion,templatename,sapuser,sappass,sapashost,sapclient,strOutPutPath,clientid):

    # Initialize variables
    dicResult = {} #Function Result Dictionary
    dictECCFieldMapping = {} 
    dictTransformationDetails = {}
    dictsapextraction = {}
    dicttblmapping = {}
    dictWriteExcel = {}
    dictXMLConvert = {}
    dictTransformationRule = {}
    dictTransformationRuleProcess = {}
        # SAP connection parameters
    connection_params = {
        'user': f'{sapuser}',
        'passwd': f'{sappass}',
        'ashost': f'{sapashost}',
        'sysnr': '00',
        'client': f'{sapclient}',
        'lang': 'EN',
        'trace': '0'  # Disable RFC logging
    }

    try:
        dicResult['iserror'] = False


        logger.info("Collecting Transformation Details From DB")
        dictTransformationDetails = fetch_transformation_details(server,database,username,password,saptepmversion,templatename)

        if dictTransformationDetails['iserror'] == True:
            dicResult['iserror'] = dictTransformationDetails['iserror']
            dicResult['error'] = dictTransformationDetails['error']
            dicResult['error_details'] = dictTransformationDetails['error_details']
            logger.error(dictTransformationDetails['error_details'])
            return dicResult

        lstTransforamtionDetails = dictTransformationDetails['value']

        logger.info("Collecting ECC Field Mapping From DB")
        dictECCFieldMapping = fetch_ecc_mapping(server,database,username,password,lstTransforamtionDetails[Transformation_details.EnumTemplateid],clientid)

        if dictECCFieldMapping['iserror'] == True:
            dicResult['iserror'] = dictECCFieldMapping['iserror']
            dicResult['error'] = dictECCFieldMapping['error']
            dicResult['error_details'] = dictECCFieldMapping['error_details']
            logger.error(dictECCFieldMapping['error_details'])
            return dicResult
        
        dfeccmapping = dictECCFieldMapping['mapping_df']
        dictECCFieldMapping = dictECCFieldMapping['value']
        
        logger.info("Details Collected from DB For ECC Field Mapping")
        logger.info("Collecting ECC Transformation Rule From DB")

        dictTransformationRule = fetch_tranformation_rule(server,database,username,password,clientid)
        if dictTransformationRule['iserror'] == True:
            dicResult['iserror'] = dictTransformationRule['iserror']
            dicResult['error'] = dictTransformationRule['error']
            dicResult['error_details'] = dictTransformationRule['error_details']
            logger.error(dictTransformationRule['error_details'])
            return dicResult

        dictTransformationRule = dictTransformationRule['value']

        templatename = str(templatename).replace(" - ","_").replace(" ","_")
        sqltblname = saptepmversion + "_" + templatename + "_"
        dictsapextraction = thread_extract_data_from_sap(connection_params,dictECCFieldMapping,sqltblname)

        if dictsapextraction['iserror'] == True:    
            dicResult['iserror'] = True
            dicResult['error'] = dictsapextraction['error']
            dicResult['error_details'] = dictsapextraction['error_details'] 
            logger.error(dictsapextraction['error_details'])
            return dicResult
        dictsapextraction = dictsapextraction['value']

        dicttblmapping = table_mapping_parallel(dfeccmapping,dictsapextraction)

        if dicttblmapping['iserror'] == True:    
            dicResult['iserror'] = True
            dicResult['error'] = dicttblmapping['error']
            dicResult['error_details'] = dicttblmapping['error_details'] 
            logger.error(dicttblmapping['error_details'])
            return dicResult
        
        dicttblmapping = dicttblmapping['value']

        dictTransformationRuleProcess = processs_transformation_rule(dictTransformationRule,dicttblmapping)
        if dictTransformationRuleProcess['iserror'] == True:    
            dicResult['iserror'] = True
            dicResult['error'] = dictTransformationRuleProcess['error']
            dicResult['error_details'] = dictTransformationRuleProcess['error_details'] 
            logger.error(dictTransformationRuleProcess['error_details'])
            return dicResult

        timestamp = datetime.now().strftime("%Y-%m-%d%H%M%S")
        strFlderName = os.path.join(strOutPutPath,str(templatename+'_'+clientid+'_'+timestamp))
        os.makedirs(strFlderName, exist_ok=True)
        strOutPutFile = os.path.join(strFlderName,templatename + "_OutPut.xml")
        dictWriteExcel = write_multiple_sheets_to_excel(dicttblmapping,lstTransforamtionDetails[Transformation_details.EnumBlankTemplatePath],strOutPutFile)
        if dictWriteExcel['iserror'] == True:    
            dicResult['iserror'] = True
            dicResult['error'] = dictWriteExcel['error']
            dicResult['error_details'] = dictWriteExcel['error_details'] 
            logger.error(dictWriteExcel['error_details'])
            return dicResult
        
        dictXMLConvert = XMLSplitter(strOutPutFile)
        if dictWriteExcel['iserror'] == True:    
            dicResult['iserror'] = True
            dicResult['error'] = dictXMLConvert['error']
            dicResult['error_details'] = dictXMLConvert['error_details']
            logger.error(dictXMLConvert['error_details']) 
            return dicResult
        
        dictConverttoZip = zip_folder(strFlderName)
        if dictConverttoZip['iserror'] == True:    
            dicResult['iserror'] = True
            dicResult['error'] = dictConverttoZip['error']
            dicResult['error_details'] = dictConverttoZip['error_details']
            logger.error(dictConverttoZip['error_details']) 
            return dicResult

        dicResult['value'] = dictConverttoZip['value']

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in Process Transformation."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
    return dicResult