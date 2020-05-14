import psycopg2
import os
import urllib.parse as up

from dotenv import load_dotenv
load_dotenv()

up.uses_netloc.append("postgres")
url = up.urlparse(os.environ["DATABASE_URL"])

labs_conn = psycopg2.connect(database=url.path[1:],
user=url.username,
password=url.password,
host=url.hostname,
port=url.port
)

labs_curs = labs_conn.cursor()

def delete_prices_raw():
    # Insert Name of Table to Delete
    delete_Q = """DROP table prices_raw;"""

    labs_curs.execute(delete_Q)
    labs_conn.commit()
    print("Table Deleted.")

    # labs_curs.close()
    # labs_conn.close()
    # print("PostgreSQL labs_connection is closed.")

if __name__ == "__main__":
    import psycopg2
    import os
    import urllib.parse as up

    from dotenv import load_dotenv
    load_dotenv()

    up.uses_netloc.append("postgres")
    url = up.urlparse(os.environ["DATABASE_URL"])

    labs_conn = psycopg2.connect(database=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
    )

    labs_curs = labs_conn.cursor()

    # Insert Name of Table to Delete
    delete_Q = """DROP table prices_raw;"""

    labs_curs.execute(delete_Q)
    labs_conn.commit()
    print("Table Deleted.")

    # TODO: Make it so connection closes 
    # even if error is raised above

    labs_curs.close()
    labs_conn.close()
    print("PostgreSQL labs_connection is closed.")