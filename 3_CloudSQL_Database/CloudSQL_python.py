# Another way to use cloudsql is using psycog2 
import psycopg2

# This is not a good idea because you are hard coding the ip of your database so don't use it
# Also your credentials so never use it
conn = psycopg2.connect(
    host='127.0.0.1', # Localhost basically
    port=5432, # The prot you have specified in the proxy connection for your localhost
    database='db_name',
    user='postgres',
    password='password',
)

cur = conn.cursor()
cur.execute('SELECT * FROM orders LIMIT 10;')
print(cur.fetchall())

cur.execute('''
    SELECT order_status,
        count(*) AS order_count
    FROM orders
    GROUP BY 1
    ORDER BY 2 DESC;
''')

print(cur.fetchall())

