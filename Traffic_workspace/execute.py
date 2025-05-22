import extract, transform, load


if __name__ == '__main__':
        
    df_crash, df_vehicle  = transform.read_data_pipeline(
        './data/raw/traffic_crashes2.csv',
        './data/raw/traffic_crashes-vehicles2.csv'
        )
    
    df_crash, df_vehicle = transform.drop_rows_with_null_values_pipeline(
        df_crash, 
        df_vehicle
        )
    
    df_merged = transform.merge_dataframes(df_vehicle, df_crash)

    load.save_df_merged(df_merged)

