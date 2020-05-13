import mysql.connector
import os

from dotenv import load_dotenv
load_dotenv()

sauti_db = mysql.connector.connect(
  host=os.environ["MYSQL_HOST"],
  user=os.environ["MYSQL_USER"],
  passwd=os.environ["MYSQL_PASSWORD"],
  database=os.environ["MYSQL_DATABASE"]
)

print(sauti_db)

mycursor = sauti_db.cursor()

Q = """SELECT * FROM `platform_market_prices2` 
ORDER BY `id`;"""

mycursor.execute(Q)

result = mycursor.fetchone()
print(result)

sauti_db.close()