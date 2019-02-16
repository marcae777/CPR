import MySQLdb
import os
from cpr_v1.SqlDb import SqlDb

passwd = "mcanoYw2E#"
database = 'hidrologia'



try:
    self = SqlDb(database,'root','localhost',passwd,3306)
    self.read_sql('show databases')
except MySQLdb.OperationalError:
    self = SqlDb('','root','localhost',passwd,3306)
try:
    self.execute_sql("CREATE DATABASE %s CHARACTER SET UTF8;"%database)
except MySQLdb.ProgrammingError:
    pass
if database in self.read_sql("show databases;").Database.values:
    pass
filepath = os.getcwd()+'tablas_siata.sql'
if 'siata_Consulta' in self.read_sql("SELECT user FROM mysql.user;")['user'].values:
    print('user already created')
else:
    statement = """CREATE USER 'siata_Consulta'@'localhost' IDENTIFIED BY 'si@t@64512_C0nsult4'"""
    self.execute_sql(statement)
    self.execute_sql("GRANT ALL PRIVILEGES ON siata.* TO 'siata_Consulta'@'localhost';")
    self.execute_sql("FLUSH PRIVILEGES")
    print('INFO: siata_Consulta user was added')
# this is to install a copy of Siata Database for production porpuses.
flag = input("Do you want to install a local siata database?")
if flag in ['yes','y','si']:
    try:
        self.execute_sql("CREATE DATABASE siata CHARACTER SET UTF8;")
        print('INFO: Database siata succesfully created')
    except MySQLdb.ProgrammingError:
        print('Warning: database exist')
    self.execute_sql("ALTER TABLE  hydro_hydrodata ADD UNIQUE (fk_id,fecha);")
    print('INFO: constraint added')
    self.dbname = 'siata'
    for line in open(filepath):
        try:
            self.execute_sql(line)
            print('INFO: TABLE CREATED')
        except MySQLdb.OperationalError:
            pass
print('Copying full history data into local mysql, it takes too much time. be patient!')
for path in info.DATA_PATH+'migration':
    insert_in_datos(info.DATA_PATH+'migration'+path)

self = Nivel(codigo=codigos[0],**info.LOCAL)
end = datetime.datetime.now()
start = end - datetime.timedelta(days = 30)
df = pd.read_csv('datos.csv',index_col=0)
df.index = pd.to_datetime(df.index)
N = 10
index = np.array(np.linspace(0,df.index.size,N),int)
dfs = []
for i in range(0,len(index)):
    try:
        df_split = df.iloc[index[i]:index[i+1]].resample('5min').max()
        df_split.columns = np.array(df_split.columns,int)
        df_split.index = pd.to_datetime(df_split.index)
        insert_hydro_hydrodata(self,df_split,'profundidad')
    except IndexError:
        print('no worries dude!')
end = datetime.datetime.now()
start = end - datetime.timedelta(days = 30)
df = self.level_all(start,end)
