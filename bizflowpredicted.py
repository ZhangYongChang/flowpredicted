import pandas as pd
import numpy as np
import logging
import logging.config
import time
import MySQLdb

from fbprophet import Prophet

log_filename = "flow_predicted_logging.log"
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a')

class FlowPredict():   
    def __init__(self, host, port, user, pswd, days):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__pswd = pswd
        self.__days = days        
    
    def __predict_flow_dic(self, dayflows):
        
        dayflows = sorted(dayflows.items(), key=lambda d:d[0])
        
        df = pd.DataFrame(data=dayflows,columns=["ds","y"])
        # 读入数据集
        #df = pd.read_csv(dayflows)
        df.head()
        
        # 拟合模型
        m = Prophet()
        m.fit(df)
        
        # 构建待预测日期数据框，periods = 365 代表除历史数据的日期外再往后推 365 天
        future = m.make_future_dataframe(periods=self.__days)
        #future.tail()
        
        # 预测数据集
        forecast = m.predict(future)
        #forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
        
        forecastflows = []
        for index, flow in forecast.iterrows():
            forecastflows.append([flow['ds'], flow['yhat'], flow['yhat_lower'], 
                                 flow['yhat_upper']])
        # 展示预测结果
        #m.plot(forecast).show()
        
        # 预测的成分分析绘图，展示预测中的趋势、周效应和年度效应
        #m.plot_components(forecast).show()
        
        #print(forecast.columns)
        return forecastflows

    def __query_hisflow_biz(self, ids):     
        # SQL 查询语句
        sql = "SELECT ccircuitid,csbizaveratio,csbizmaxratio,crbizaveratio,\
        crbizmaxratio, cstattime FROM t_biztopflow WHERE ccircuitid in (%s)" % (",".join(str(i) for i in ids))
        
        saveflows = {}  #
        smaxflows = {}  #
        raveflows = {}  #
        rmaxflows = {}  #
        
        idsaveflows = {}  # ID关联的每日发送均值流速
        idsmaxflows = {}  # ID关联的每日发送峰值流速
        idraveflows = {}  # ID关联的每日接收均值流速
        idrmaxflows = {}  # ID关联的每日接收均值流速
        ids = set()       # ID集合
        
        # 打开数据库连接
        db = MySQLdb.connect(host=self.__host, port=self.__port, user=self.__user, 
                             passwd=self.__pswd, db="resanalysisdb", charset='utf8')

        # 使用cursor()方法获取操作游标 
        cursor = db.cursor()
        
        try:
            # 执行SQL语句
            cursor.execute(sql)
            # 获取所有记录列表
            results = cursor.fetchall()
            count = 0
            for row in results:
               id = row[0] 
               time = row[5].isoformat()
               saveflows[time] = row[1]
               idsaveflows[id] = saveflows
               smaxflows[time] = row[2]
               idsmaxflows[id] = smaxflows
               raveflows[time] = row[3]
               idraveflows[id] = raveflows
               rmaxflows[time] = row[4]
               idrmaxflows[id] = rmaxflows
               ids.add(id)
               count = count + 1
            logging.info("number of this batch datasize:%d" % count)
    
        except Exception as e:
            print (e)
            print ("Error: unable to fecth data")
        
        # 关闭数据库连接
        db.close()
        
        return ids, idsaveflows, idsmaxflows, idraveflows, idrmaxflows
        
    def __save_foreflow_biz(self, idforeflows, type):
        sqlflows = []
        # SQL 查询语句
        sql = "insert into t_forecast_biz(id,time,type,yhat,yhat_lower,yhat_upper) \
        values (%s, %s, %s, %s, %s, %s)"
        
        for id,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((id, flow[0], type, flow[1], flow[2], flow[3]))
        
        # 打开数据库连接
        db = MySQLdb.connect(host=self.__host, port=self.__port, user=self.__user, 
                             passwd=self.__pswd, db="resanalysisdb", charset='utf8')

        # 使用cursor()方法获取操作游标 
        cursor = db.cursor()
        
        try:
            # 执行SQL语句
            cursor.executemany(sql, sqlflows)
        except Exception as e:
            print (e)
        
        db.commit()
        cursor.close()
        # 关闭数据库连接
        db.close()
    
    def predict_biz(self, ids):
        # 根据查询历史流量
        ids, idsaveflows, idsmaxflows, idraveflows, idrmaxflows = self.__query_hisflow_biz(ids)
        logging.info("ids=%s" % ids)
        
        idsforeaveflows = {}  #
        idsforemaxflows = {}  #
        idrforeaveflows = {}  #
        idrforemaxflows = {}  #
        
        # 遍历ID，按各指标计算趋势流量
        for id in ids:
            idsforeaveflows[id] = self.__predict_flow_dic(idsaveflows[id])
            idsforemaxflows[id] = self.__predict_flow_dic(idsmaxflows[id])
            idrforeaveflows[id] = self.__predict_flow_dic(idraveflows[id])
            idrforemaxflows[id] = self.__predict_flow_dic(idrmaxflows[id])
        
        # 趋势流量入库
        self.__save_foreflow_biz(idsforeaveflows, 1)
        self.__save_foreflow_biz(idsforemaxflows, 2)
        self.__save_foreflow_biz(idrforeaveflows, 3)
        self.__save_foreflow_biz(idrforemaxflows, 4)


def biz_flow_predicted():
    logging.info("begin biz flow predicted...")

    ids = [504365114,504365122]
    
    predict = FlowPredict(host="localhost", port=3306, user="root", pswd="vislecaina", days=30)    
    predict.predict_biz(ids)

    logging.info("end biz flow predicted...")

if __name__ == "__main__":
    biz_flow_predicted()