import transform
import logging as lg


def save_df_merged(df):
    path = 'data/raw/crashes_merged.csv'
    df.to_csv(path, index=False)
    lg.info(f"crashes_merged salvo!")