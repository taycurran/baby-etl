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

create_table_Q = """
                 CREATE TABLE IF NOT EXISTS prices (
                     id SERIAL PRIMARY KEY,
                     source VARCHAR(200),
                     country VARCHAR(5),
                     market VARCHAR(25),
                     product_cat VARCHAR(50),
                     product_agg VARCHAR(50),
                     product VARCHAR(50),
                     date DATE,
                     retail INTEGER,
                     wholesale INTEGER,
                     currency VARCHAR(5),
                     unit VARCHAR(5),
                     active VARCHAR(5),
                     udate VARCHAR(5)
                 );
                 """
curs.execute(create_table_Q)
conn.commit()
print("Table Created Successfully")

curs.close()
conn.close()
print("PostgreSQL connection is closed.")
