import psycopg2
import os
import urllib.parse as up

from dotenv import load_dotenv
load_dotenv()

up.uses_netloc.append("postgres")
url = up.urlparse(os.environ["DATABASE_URL"])

conn = psycopg2.connect(database=url.path[1:],
user=url.username,
password=url.password,
host=url.hostname,
port=url.port
)

curs = conn.cursor()

delete_Q = """DROP table passengers;"""

curs.execute(delete_Q)
conn.commit()
print("Table Deleted.")

curs.close()
conn.close()
print("PostgreSQL connection is closed.")
