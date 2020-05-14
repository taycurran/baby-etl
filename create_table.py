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

# ?-- Should we create our own DB id? --?

def create_prices_raw():
    create_table_Q = """
                    CREATE TABLE IF NOT EXISTS prices_raw (
                        id_sauti INTEGER,
                        source VARCHAR(200),
                        country VARCHAR(5),
                        market VARCHAR(25),
                        product_cat VARCHAR(50),
                        product_agg VARCHAR(50),
                        product VARCHAR(50),
                        date VARCHAR(50),
                        retail INTEGER,
                        wholesale INTEGER,
                        currency VARCHAR(5),
                        unit VARCHAR(5),
                        active VARCHAR(5),
                        udate VARCHAR(5)
                    );
                    """
    labs_curs.execute(create_table_Q)
    labs_conn.commit()
    print("Table Created Successfully.")

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

    create_prices_raw()

    labs_curs.close()
    labs_conn.close()
    print("PostgreSQL labs_connection is closed.") 

