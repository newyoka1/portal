import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

my = get_conn('donors_2024')
cur = my.cursor()

print("Dropping and recreating donor_party_totals...")
cur.execute('DROP TABLE IF EXISTS donor_party_totals')
cur.execute('''
CREATE TABLE donor_party_totals (
  sboeid VARCHAR(20) PRIMARY KEY,
  StandardIndivVal INT,
  FULLNAME VARCHAR(200),
  FIRSTNAME VARCHAR(50),
  LASTNAME VARCHAR(50),
  CITY VARCHAR(60),
  STATE VARCHAR(25),
  ZIPCODE VARCHAR(10),
  voterparty VARCHAR(3),
  Countyname VARCHAR(255),
  adval INT,
  sdval INT,
  cdval INT,
  total_rep DECIMAL(12,2),
  total_dem DECIMAL(12,2),
  total_oth DECIMAL(12,2),
  grand_total DECIMAL(12,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
''')
my.commit()
print("Table created. Building totals with GROUP BY to collapse duplicates...")

cur.execute('''
INSERT INTO donor_party_totals
SELECT
  sboeid,
  MAX(StandardIndivVal),
  MAX(FULLNAME),
  MAX(FIRSTNAME),
  MAX(LASTNAME),
  MAX(CITY),
  MAX(STATE),
  MAX(ZIPCODE),
  MAX(voterparty),
  MAX(Countyname),
  MAX(adval),
  MAX(sdval),
  MAX(cdval),
  ROUND(SUM(
    COALESCE(CAST(R2018amt AS DECIMAL(12,2)),0)+COALESCE(CAST(R2019amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(R2020amt AS DECIMAL(12,2)),0)+COALESCE(CAST(R2021amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(R2022amt AS DECIMAL(12,2)),0)+COALESCE(CAST(R2023amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(R2024amt AS DECIMAL(12,2)),0)
  ), 2) AS total_rep,
  ROUND(SUM(
    COALESCE(CAST(D2018amt AS DECIMAL(12,2)),0)+COALESCE(CAST(D2019amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2020amt AS DECIMAL(12,2)),0)+COALESCE(CAST(D2021amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2022amt AS DECIMAL(12,2)),0)+COALESCE(CAST(D2023amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2024amt AS DECIMAL(12,2)),0)
  ), 2) AS total_dem,
  ROUND(SUM(
    COALESCE(CAST(U2018amt AS DECIMAL(12,2)),0)+COALESCE(CAST(U2019amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2020amt AS DECIMAL(12,2)),0)+COALESCE(CAST(U2021amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2022amt AS DECIMAL(12,2)),0)+COALESCE(CAST(U2023amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2024amt AS DECIMAL(12,2)),0)
  ), 2) AS total_oth,
  ROUND(SUM(
    COALESCE(CAST(R2018amt AS DECIMAL(12,2)),0)+COALESCE(CAST(R2019amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(R2020amt AS DECIMAL(12,2)),0)+COALESCE(CAST(R2021amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(R2022amt AS DECIMAL(12,2)),0)+COALESCE(CAST(R2023amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(R2024amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2018amt AS DECIMAL(12,2)),0)+COALESCE(CAST(D2019amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2020amt AS DECIMAL(12,2)),0)+COALESCE(CAST(D2021amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2022amt AS DECIMAL(12,2)),0)+COALESCE(CAST(D2023amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(D2024amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2018amt AS DECIMAL(12,2)),0)+COALESCE(CAST(U2019amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2020amt AS DECIMAL(12,2)),0)+COALESCE(CAST(U2021amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2022amt AS DECIMAL(12,2)),0)+COALESCE(CAST(U2023amt AS DECIMAL(12,2)),0)+
    COALESCE(CAST(U2024amt AS DECIMAL(12,2)),0)
  ), 2) AS grand_total
FROM ProvenDonors2024OnePerInd
WHERE sboeid IS NOT NULL AND sboeid != ''
GROUP BY sboeid
''')
my.commit()
print("Insert complete.")

# Validation
cur.execute('SELECT COUNT(*) FROM donor_party_totals')
print(f'Unique donors in summary table: {cur.fetchone()[0]:,}')

cur.execute('SELECT SUM(total_rep), SUM(total_dem), SUM(total_oth), SUM(grand_total) FROM donor_party_totals')
r = cur.fetchone()
print(f'\nTotal donated to Republicans  : ${r[0]:,.2f}')
print(f'Total donated to Democrats    : ${r[1]:,.2f}')
print(f'Total donated to Other/Unknown: ${r[2]:,.2f}')
print(f'Grand Total (all parties)     : ${r[3]:,.2f}')

print('\nTop 10 donors by grand total:')
cur.execute('''
  SELECT FULLNAME, CITY, voterparty, total_rep, total_dem, total_oth, grand_total 
  FROM donor_party_totals ORDER BY grand_total DESC LIMIT 10
''')
for row in cur.fetchall():
    print(f'  {str(row[0]):<30} {str(row[1]):<20} {str(row[2]):<5}  REP:${row[3]:>10,.2f}  DEM:${row[4]:>10,.2f}  OTH:${row[5]:>10,.2f}  TOTAL:${row[6]:>10,.2f}')

print('\ndone - table donor_party_totals is ready.')
my.close()