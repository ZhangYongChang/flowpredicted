import pymysql
import json

#mysql interface
class Pymsql(object):
    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db

    def __getConnection(self):
        if not self.db:
            raise NameError("database is null")

        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')
        cur = self.conn.cursor()
        if not cur:
            raise NameError("connect database falied.")
        else:
            return cur

    def executeQuery(self, sql, param):
        cur = self.__getConnection()
        cur.execute(sql)
        result = cur.fetchall()
        self.conn.close()
        return result

    def executeInsert(self, sql, param):
        cur = self.__getConnection()
        cur.executemany(sql, param)
        self.conn.commit()
        self.conn.close()

class DaoConfig(object):
    def __init__(self, dbname):
        self.remoteIp = "10.170.103.77"
        self.port = 51028
        self.username = "root"
        self.passwd = "vislecaina"
        self.db = dbname
    
    def getDaoConfig(self):
        return self.remoteIp, self.port, self.username, self.passwd, self.db

    def getDao(self):
        return Pymsql(self.remoteIp, self.port, self.username, self.passwd, self.db)


def get_res_dao():
    daoConfig = DaoConfig("resanalysisdb")
    return daoConfig.getDao()

def get_topo_dao():
    daoConfig = DaoConfig("nmstmdb")
    return daoConfig.getDao()