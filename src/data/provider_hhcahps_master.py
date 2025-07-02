import pandas as pd
import numpy as np
import sqlite3
import os
from typing import Dict

import json


class CMSDataProcessor:
    def __init__(self, data_dir: str, db_path: str = None):
        self.data_dir = data_dir
        if db_path is None:
            db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

    def load_raw_data(self, hhcahps_provider_file: str, hh_provider_file: str, hh_zip_file: str) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        files = {
            'hhcahps_provider': hhcahps_provider_file,
            'hh_provider': hh_provider_file,
            'hh_zip': hh_zip_file
        }
        for key, filename in files.items():
            filepath = os.path.join(self.data_dir, filename)
            dataframes[key] = pd.read_csv(filepath, low_memory=False)
        return dataframes

    def clean_and_standardize_data(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        # Clean HHCAHPS Provider data
        df = dataframes['hhcahps_provider']
        df['CCN'] = df['CMS Certification Number (CCN)'].astype(str).str.zfill(6)
        dataframes['hhcahps_provider'] = df
        # Clean HH Provider data
        df = dataframes['hh_provider']
        df['CCN'] = df['CMS Certification Number (CCN)'].astype(str).str.zfill(6)
        df['ZIP Code'] = df['ZIP Code'].astype(str).str.replace('-', '').str[:5]
        dataframes['hh_provider'] = df
        # Clean ZIP data
        df = dataframes['hh_zip']
        df['CCN'] = df['CMS Certification Number (CCN)'].astype(str).str.zfill(6)
        df['ZIP Code'] = df['ZIP Code'].astype(str).str.replace('-', '').str[:5]
        dataframes['hh_zip'] = df
        return dataframes

    def create_master_provider_dataset(self, dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        master_df = dataframes['hh_provider'].copy()
        # Merge with HHCAHPS quality data
        hhcahps_cols = [
            'CCN',
            'HHCAHPS Survey Summary Star Rating',
            'Number of completed Surveys',
            'Survey response rate'
        ]
        hhcahps_df = dataframes['hhcahps_provider'][hhcahps_cols].copy()
        master_df = master_df.merge(hhcahps_df, on='CCN', how='left', suffixes=('', '_hhcahps'))
        # Add service area information (ZIP codes served)
        zip_counts = dataframes['hh_zip'].groupby('CCN')['ZIP Code'].agg(['count', 'nunique']).reset_index()
        zip_counts.columns = ['CCN', 'total_zip_records', 'unique_zips_served']
        master_df = master_df.merge(zip_counts, on='CCN', how='left')
        return master_df

    def save_to_database(self, master_df: pd.DataFrame, table_name: str = 'provider_hhcahps'):
        master_df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        self.conn.commit()

if __name__ == "__main__":
    # You can change these filenames as needed
    # Set data_dir to always be root-relative: <project_root>/data/cms_hh_quality
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(project_root, 'data', 'cms_hh_quality')
    # Read file names from data/datamap.json
    datamap_path = os.path.join(project_root, 'data', 'datamap.json')
    with open(datamap_path, 'r') as f:
        datamap = json.load(f)

    hhcahps_provider_file = os.path.join(project_root, datamap['hhcahps_provider'])
    hh_provider_file = os.path.join(project_root, datamap['hh_provider'])
    hh_zip_file = os.path.join(project_root, datamap['hh_zip'])

    db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
    processor = CMSDataProcessor(data_dir, db_path)

    dfs = processor.load_raw_data(hhcahps_provider_file, hh_provider_file, hh_zip_file)
    dfs = processor.clean_and_standardize_data(dfs)
    master = processor.create_master_provider_dataset(dfs)

    processor.save_to_database(master, table_name='provider_hhcahps')

    print(master.head())
    print(f"Loaded {len(master)} records into provider_hhcahps table.")
