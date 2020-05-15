# For Sauti MySQL Database
import mysql.connector

# For labs_curs PostgreSQL Database
import urllib.parse as up
import psycopg2
from create_table import create_prices_raw
from delete_table import delete_prices_raw

# For Both
import os
from dotenv import load_dotenv
load_dotenv()

# --- Sauti DB Connection ---

sauti_conn = mysql.connector.connect(
  host=os.environ["MYSQL_HOST"],
  user=os.environ["MYSQL_USER"],
  passwd=os.environ["MYSQL_PASSWORD"],
  database=os.environ["MYSQL_DATABASE"]
)

# buffered=True made this script finally work
# ?-- Not sure exactly what it did though --?
sauti_curs = sauti_conn.cursor(buffered=True)

# --- labs_curs DB Connection ---

up.uses_netloc.append("postgres")
url = up.urlparse(os.environ["DATABASE_URL"])

labs_conn = psycopg2.connect(database=url.path[1:],
user=url.username,
password=url.password,
host=url.hostname,
port=url.port
)

labs_curs =labs_conn.cursor()

# Drop Table If Exists
delete_prices_raw()
# Recreate Table
create_prices_raw()

# ---

Q = """SELECT * FROM platform_market_prices2;"""

sauti_curs.execute(Q)

rows = sauti_curs.fetchmany(10)

for row in rows:
  insert_row = """
  INSERT INTO prices_raw
  (id_sauti, source, country, market, product_cat,
  product_agg, product, date, retail, wholesale,
  currency, unit, active, udate) VALUES 
  (%(id_sauti)s, %(source)s, %(country)s, %(market)s, %(product_cat)s, 
  %(product_agg)s, %(product)s, %(date)s, %(retail)s, %(wholesale)s, 
  %(currency)s, %(unit)s, %(active)s, %(udate)s);"""

  vals = {'id_sauti': row[0],
          'source': row[1],
          'country': row[2],
          'market': row[3],
          'product_cat': row[4],
          'product_agg': row[5],
          'product': row[6],
          'date': row[7],
          'retail': row[8],
          'wholesale': row[9],
          'currency': row[10],
          'unit': row[11],
          'active': row[12],
          'udate': row[13]
         }
  labs_curs.execute(insert_row, vals)



labs_conn.commit()

labs_curs.close()
labs_conn.close()

print("DS DB Connection Closed.")

sauti_curs.close()
sauti_conn.close()

print("Sauti DB Connection Closed.")