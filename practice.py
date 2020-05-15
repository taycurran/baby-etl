import mysql.connector
import os

from dotenv import load_dotenv
load_dotenv()

sauti_conn = mysql.connector.connect(
  host=os.environ["MYSQL_HOST"],
  user=os.environ["MYSQL_USER"],
  passwd=os.environ["MYSQL_PASSWORD"],
  database=os.environ["MYSQL_DATABASE"]
)

sauti_curs = sauti_conn.cursor()

# TODO: Change Formatting within Query for Date Column


Q = """SELECT date FROM platform_market_prices2
        TO_CHAR(DATETIME, 'MM-DD-YYYY');"""

Q2 = """SELECT date FROM platform_market_prices2
        CONVERT(VARCHAR, GETDATE());"""

Q3 = """SELECT date FROM platform_market_prices2
        CAST(GETDATE() AS VARCHAR(100));"""

Q4 = """SELECT * except date FROM platform_market_prices2;"""

Q5 = """SELECT id, 
        source,
        country,
        market,
        product_cat,
        product_agg,
        product,
        retail,
        wholesale,
        currency,
        unit,
        active
        from platform_market_prices2;
     """

sauti_curs.execute(Q5)

rows = sauti_curs.fetchmany(2)

for row in rows:
  insert_row = """
  INSERT INTO prices_raw
  (id_sauti, source, country, market, product_cat,
  product_agg, product, retail, wholesale,
  currency, unit, active) VALUES """ + str(row) + """;"""
  print(insert_row)
  labs_curs()



# from DateTime import Timezones

# result = sauti_curs.fetchmany(5)

# print(result)

# sauti_conn.close()