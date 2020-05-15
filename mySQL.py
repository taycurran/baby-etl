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

Q = """SELECT * FROM platform_market_prices2;"""
# TODO: Change Formatting within Query for Date Column

sauti_curs.execute(Q)


result = sauti_curs.fetchmany(10)

print(result)

sauti_conn.close()