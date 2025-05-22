import pandas as pd
import logging as lg

lg.basicConfig(level=lg.DEBUG, format='%(levelname)s: %(message)s')

def read_datasources(source_name):
    
    if source_name.endswith(".csv"):
        lg.info(f"Lendo {source_name} ...")
        return pd.read_csv(source_name)
    
    elif source_name.endswith((".xls")):
        lg.info(f"Lendo {source_name} ...")
        return pd.read_excel(source_name)
    
    elif source_name.endswith((".xlsx")):
        lg.info(f"Lendo {source_name} ...")
        return pd.read_excel(source_name)
    else:
        raise ValueError("Formato de arquivo não suportado.")

def read_data_pipeline(crash_file, vehicle_file):
    df_crash = read_datasources(crash_file)
    df_vehicle = read_datasources(vehicle_file)
    return df_crash, df_vehicle


#################################################################


def drop_rows_with_null_values(df, rows_to_drop_quantity = 2):
    df.dropna(axis='columns', how='all', inplace=True)
    lg.info("Dropando colunas vazias...")

    df.dropna(axis='index', thresh=rows_to_drop_quantity, inplace=True)
    lg.info(f"Dropando linhas com {rows_to_drop_quantity} colunas vazias...")

    return df

def drop_rows_with_null_values_pipeline(df_crash, df_vehicle, rows_to_drop_quantity = 2):
    rows_before_dfcrash, rows_before_dfvehicle = len(df_crash), len(df_vehicle)

    df_crash = drop_rows_with_null_values(df_crash, rows_to_drop_quantity)
    df_vehicle = drop_rows_with_null_values(df_vehicle, rows_to_drop_quantity)

    rows_after_dfcrash, rows_after_dfvehicle = len(df_crash), len(df_vehicle)

    lg.info(f'{rows_before_dfcrash - rows_after_dfcrash} linhas afetadas em df_crash')
    lg.info(f'{rows_before_dfvehicle - rows_after_dfvehicle} linhas afetadas em df_vehicle')

    return df_crash, df_vehicle


#################################################################


def fill_missing_values(df):
    pass

def fill_missing_values_pipeline(df_crash, df_vehicle):
    pass


#################################################################


def merge_dataframes(df_vehicles, df_crashes):
    df_merged = df_vehicles.merge(df_crashes, how='left', on='crash_record_id', suffixes=('_left','_right'))
    return df_merged


def merge_dataframes_pipeline(df_crash, df_vehicle):
    pass


#################################################################


def format_dataframes_pipeline(df_agg):
    pass

def rename_columns(df):
    pass



#################################################################


# df_crashes = pd.read_csv('./data/raw/traffic_crashes2.csv')
# df_crashes_vehicles = pd.read_csv('./data/raw/traffic_crashes-vehicles2.csv')

# print(df_crashes_vehicles.columns, df_crashes.columns)
# df_merged = df_crashes.merge(df_crashes_vehicles, how='left', on='crash_record_id', suffixes=('_left','_right'))

# print(df_merged)
# print(df_merged.groupby('vehicle_type').agg({'crash_record_id': 'count'}).reset_index())
