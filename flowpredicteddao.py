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

    # 创建连接，获取连接
    def __getConnection(self):
        if not self.db:
            raise NameError("database is null")

        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')
        cur = self.conn.cursor()
        if not cur:
            raise NameError("connect database falied.")
        else:
            return cur

    #执行SQL语句
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

# 数据库接口
class DaoConfig(object):
    # 数据库配置信息
    def __init__(self):
        self.remoteIp = "localhost"
        self.port = 3306
        self.username = "root"
        self.passwd = "vislecaina"
        self.db = "resanalysisdb"
    
    # 数据库配置信息
    def getDaoConfig(self):
        return self.remoteIp, self.port, self.username, self.passwd, self.db

# 获取数据库访问对象
def get_dao():
    daoConfig = DaoConfig()
    remoteIp = daoConfig.remoteIp
    port = daoConfig.port
    username = daoConfig.username
    passwd = daoConfig.passwd
    db = daoConfig.db
    dao = Pymsql(remoteIp, port, username, passwd, db)
    return dao