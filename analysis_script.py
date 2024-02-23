import time
import pandas as pd
from google.cloud import bigquery

gcp_project = 'ba-glass-hack24por-2001'
bq_dataset = 'BA'
table_id = "ba-glass-hack24por-2001.BA.Analysis_Table"

client = bigquery.Client(project=gcp_project)

job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
)

def gcp2df(sql):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()

query_clean_ISMachine_timing = """
CREATE OR REPLACE TABLE BA.Clean_PTM AS
SELECT ROW_NUMBER() OVER(ORDER BY Date) AS Id, Line, Date, Reference, Shift,
PTM, Start_time, End_time, JobNumber,
  LAG((Start_time != 0 AND Start_time != 80000 AND Start_time != 160000) 
  OR (End_time != 0 AND End_time != 80000 AND End_time != 160000), 1) 
  OVER(ORDER BY Date ASC, JobNumber ASC, Reference ASC, Start_time ASC) OR PTM > 100 OR PTM = 0 AS lag
FROM `ba-glass-hack24por-2001.BA.PTM`
ORDER BY Date ASC, JobNumber ASC, Start_time ASC;

DELETE FROM BA.Clean_PTM 
WHERE ( Start_time != 0 AND Start_time != 80000 AND Start_time != 160000) 
OR (End_time != 0 AND End_time != 80000 AND End_time != 160000) OR lag = true;

ALTER TABLE BA.Clean_PTM DROP COLUMN lag;

CREATE OR REPLACE TABLE BA.Final_Table AS
SELECT
`ba-glass-hack24por-2001.BA.Analysis_Table`.*,
`ba-glass-hack24por-2001.BA.Clean_PTM`.Shift,
`ba-glass-hack24por-2001.BA.Clean_PTM`.JobNumber,
`ba-glass-hack24por-2001.BA.Clean_PTM`.PTM,
(SELECT PTM FROM `ba-glass-hack24por-2001.BA.Clean_PTM` WHERE 
EXTRACT(year FROM timestamp) = CAST(Date / 10000 AS int64) 
AND EXTRACT(month FROM timestamp) = MOD(CAST(Date/100 AS int64), 100)
AND EXTRACT(day FROM timestamp) = MOD(Date, 100)
AND EXTRACT(hour FROM timestamp) >= CAST(Start_time / 10000 - 1 AS int64) 
AND EXTRACT(hour FROM timestamp) < CAST(End_time / 10000 - 1 AS int64)
) AS PTM_A
FROM `ba-glass-hack24por-2001.BA.Analysis_Table`
INNER JOIN `ba-glass-hack24por-2001.BA.Clean_PTM`
ON EXTRACT(year FROM timestamp) = CAST(Date / 10000 AS int64) 
AND EXTRACT(month FROM timestamp) = MOD(CAST(Date/100 AS int64), 100)
AND EXTRACT(day FROM timestamp) = MOD(Date, 100)
AND EXTRACT(hour FROM timestamp) >= CAST(Start_time / 10000 AS int64) 
AND EXTRACT(hour FROM timestamp) < CAST(End_time / 10000 AS int64);

ALTER TABLE BA.Final_Table DROP COLUMN int64_field_0;
"""


query_IsMachine_Timing = """SELECT * FROM `ba-glass-hack24por-2001.BA.ISMachine_timings`
ORDER BY timestamp"""

query_drop = """DROP TABLE `ba-glass-hack24por-2001.BA.Analysis_Table`"""

query_best_configs = """CREATE OR REPLACE TABLE BA.Best_Configs AS
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 1) LIMIT 1;

INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 2) LIMIT 1;
INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 3) LIMIT 1;INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 4) LIMIT 1;INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 5) LIMIT 1;
INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 6) LIMIT 1;INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 7) LIMIT 1;
INSERT INTO `ba-glass-hack24por-2001.BA.Best_Configs`
SELECT * FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE PTM_a = (SELECT MAX(PTM_A) FROM `ba-glass-hack24por-2001.BA.Final_Table` WHERE Reference = 8) LIMIT 1;

ALTER TABLE `ba-glass-hack24por-2001.BA.Best_Configs`
DROP COLUMN timestamp;
ALTER TABLE `ba-glass-hack24por-2001.BA.Best_Configs`
DROP COLUMN Section;
ALTER TABLE `ba-glass-hack24por-2001.BA.Best_Configs`
DROP COLUMN PTM;
ALTER TABLE `ba-glass-hack24por-2001.BA.Best_Configs`
DROP COLUMN Shift;
ALTER TABLE `ba-glass-hack24por-2001.BA.Best_Configs`
DROP COLUMN JobNumber;
"""


##original_df = gcp2df(query_Std_Timing)
##original_df['Timestamp'] = pd.Timestamp('2023-01-01 00:01')
##original_df['Reference'] = 0
##original_df['Line'] = 'BA04'
##original_df.loc[original_df['Cavity'].isna(), 'Cavity'] = 0
##pivot_numeric = original_df.pivot_table(index=['Timestamp', 'Section', 'Reference', 'Line'], columns=['EventID'], values=numeric_columns, aggfunc='sum')
##pivot_numeric.columns = [f'E{col[1]}_On' if 'OnAngle' in col else f'E{col[1]}_Off' for col in pivot_numeric.columns]
##pivot_numeric.reset_index(inplace=True)

print("Loading ISMachine_timings Table")
df = gcp2df(query_IsMachine_Timing)
df.loc[df['Cavity'].isna(), 'Cavity'] = 0
numeric_columns = ['OnAngle', 'OffAngle']
df = df.pivot_table(index=['timestamp', 'Section', 'Reference', 'Line'], columns=['ID'], values=numeric_columns, aggfunc='sum')
df.columns = [f'E{col[1]}_On' if 'OnAngle' in col else f'E{col[1]}_Off' for col in df.columns]
df.reset_index(inplace=True)

for col in df.columns:
        if col != 'timestamp' and col != 'Section' and col != 'Reference' and col != 'Line':
                df[col] = df[col].astype(float)

df.sort_values(by=['Section', 'timestamp'], inplace=True)

cols_to_fill = ['E1_Off', 'E2_Off', 'E3_Off', 'E4_Off', 'E5_Off', 'E6_Off', 'E7_Off', 'E9_Off', 'E10_Off', 
'E11_Off', 'E12_Off', 'E13_Off', 'E14_Off', 'E15_Off', 'E16_Off', 'E17_Off', 'E18_Off', 'E19_Off', 'E20_Off',
 'E21_Off', 'E23_Off', 'E24_Off', 'E25_Off', 'E26_Off', 'E31_Off', 'E32_Off', 'E33_Off', 'E34_Off', 'E35_Off',
  'E36_Off', 'E37_Off', 'E38_Off', 'E39_Off', 'E40_Off', 'E41_Off', 'E43_Off', 'E47_Off', 'E48_Off', 'E49_Off',
   'E50_Off', 'E51_Off', 'E52_Off', 'E54_Off', 'E57_Off', 'E59_Off', 'E60_Off', 'E62_Off', 'E66_Off', 'E79_Off',
    'E80_Off', 'E1_On', 'E2_On', 'E3_On', 'E4_On', 'E5_On', 'E6_On', 'E7_On', 'E9_On', 'E10_On', 'E11_On', 'E12_On',
     'E13_On', 'E14_On', 'E15_On', 'E16_On', 'E17_On', 'E18_On', 'E19_On', 'E20_On', 'E21_On', 'E23_On', 'E24_On',
      'E25_On', 'E26_On', 'E31_On', 'E32_On', 'E33_On', 'E34_On', 'E35_On', 'E36_On', 'E37_On', 'E38_On', 'E39_On',
       'E40_On', 'E41_On', 'E43_On', 'E47_On', 'E48_On', 'E49_On', 'E50_On', 'E51_On', 'E52_On', 'E54_On', 'E57_On',
        'E59_On', 'E60_On', 'E62_On', 'E66_On', 'E79_On', 'E80_On']

df[cols_to_fill] = df.groupby('Section')[cols_to_fill].ffill()
df.reset_index(drop=True, inplace=True)

df[cols_to_fill] = df.groupby('Section')[cols_to_fill].bfill()
df.reset_index(drop=True, inplace=True)

df.to_csv('production_config.csv')



print("Creating Table Analysis_Table")
with open(r'production_config.csv', "rb") as source_file:
       job = client.load_table_from_file(source_file, table_id, job_config=job_config)
print("STATUS1:")
while job.state != 'DONE':
        time.sleep(2)
        job.reload()
        print(job.state)


print("Loading query to create the final table")
gcp2df(query_clean_ISMachine_timing)
print("DONE")


print("Drop Table Analysis_Table")
gcp2df(query_drop)

gcp2df(query_best_configs)