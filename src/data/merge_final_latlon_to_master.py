import pandas as pd
import sqlite3
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
master_csv = os.path.join(data_dir, 'masterprovider_with_penetration.csv')
fin_geo_csv = os.path.join(data_dir, 'geocoded_addresses_fin.csv')
output_table = 'new_final_master_provider'

cols = ['ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']

# 1. Load masterprovider_with_penetration.csv
master = pd.read_csv(master_csv, dtype=str)
master['ZIP CODE'] = master['ZIP CODE'].astype(str).str[:5].str.zfill(5)

# 2. Load geocoded_addresses_fin.csv
fin_geo = pd.read_csv(fin_geo_csv, dtype=str)
fin_geo['ZIP CODE'] = fin_geo['ZIP CODE'].astype(str).str[:5].str.zfill(5)

# 3. Merge lat/lon and updated_by_google into master
merged = master.merge(
    fin_geo[cols + ['lat', 'lon', 'updated_by_google']],
    on=cols,
    how='left'
)

# 4. Save to new table in DB
db = sqlite3.connect(db_path)
drop_sql = f"DROP TABLE IF EXISTS {output_table}"
db.execute(drop_sql)
merged.to_sql(output_table, db, index=False)
db.close()

# 5. Also export to CSV
output_csv = os.path.join(data_dir, 'new_final_master_provider.csv')
merged.to_csv(output_csv, index=False)
print(f"Merged masterprovider_with_penetration with final geocodes. Saved to table '{output_table}' and {output_csv}")
