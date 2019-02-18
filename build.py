import MySQLdb
import os
from cpr_v1.SqlDb import SqlDb
from cpr_v1.settings import LOCAL
#test
try:
    self = SqlDb(**LOCAL)
    self.read_sql('show databases')
    print('database exists')
except MySQLdb.OperationalError:
    LOCAL['passwd'] = ''
    self = SqlDb(**LOCAL)
    print('Error: Please install local database with cprweb')

try:
    self.execute_sql("CREATE DATABASE siata CHARACTER SET UTF8;")
    print('INFO: Database siata succesfully created')
except MySQLdb.ProgrammingError:
    print('Warning: database exist')

# Adding necessary constraint to the database
self.execute_sql("ALTER TABLE  hydro_hydrodata ADD UNIQUE (fk_id,fecha);")
print('INFO: constraint added')

self.dbname = 'siata'
for create_table_query in open('tablas_siata.sql'):
    try:
        self.execute_sql(create_table_query)
        print('INFO: TABLE CREATED')
    except MySQLdb.OperationalError:
        print('Table already exist')
