
import pandas as pd
from .logger import Logger
import time



logger = Logger.get_logger()

def log_execution_time(func):
    """
    Decorator to log the total execution time of a function.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Started: {func.__name__}")

        result = func(*args, **kwargs)

        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Completed: {func.__name__} in {elapsed_time:.2f} seconds")
        return result

    return wrapper


def process_target_table(target_table, group, source_data):
    """
    Process a single target table mapping in parallel.
    """
    try:
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

            # Merge only if the target field is not present
            if target_field not in target_df.columns:
                target_df = pd.merge(target_df, temp_df, how='left', on=target_join_cols)
                logger.debug(f"Merged {source_table} into {target_table} on {target_join_cols}")

        logger.info(f"Completed mapping for TargetTable: {target_table}")
        return target_table, target_df

    except Exception as e:
        logger.exception(f"Error processing {target_table}: {e}")
        return target_table, None

@log_execution_time
def table_mapping_parallel(dfeccmapping, source_data, num_workers=None):
    """
    Parallel processing version of table_mapping using ProcessPoolExecutor.
    """
    dicResult = {'iserror': False}
    final_target_dataframes = {}

    try:
        logger.info("Starting table_mapping_parallel function.")

        # Extract required columns
        dfmapping_main = dfeccmapping[['SoruceTable', 'SoruceField', 'TargetTable', 'TargetField', 'SoruceJoinFiled', 'TargetJoinField', 'IsMainTable']]
        target_groups = dfmapping_main.groupby('TargetTable')
        logger.info("table_mapping_parallel: Required Columns Extracted")

        for target_table, group in target_groups:
            target_table, target_df = process_target_table(target_table, group, source_data)
            if target_df is not None:
                final_target_dataframes[target_table] = target_df
            else:
                logger.info(f"table_mapping_parallel: {target_table} target df is none ")
                dicResult['iserror'] = True
                dicResult['error'] = "Unhandled Error in table_mapping_parallel: {target_table} target df is none."
                dicResult['error_details'] = f"{dicResult['error']}"
                return dicResult

        logger.info("table_mapping_parallel: Process Completed ")

        dicResult['value'] = final_target_dataframes
        logger.info("table mapping completed successfully.")

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error in ECC Table Mapping."
        dicResult['error_details'] = f"{dicResult['error']} | {str(e)}"
        logger.exception(f"Error in table_mapping_parallel: {e}")

    return dicResult
