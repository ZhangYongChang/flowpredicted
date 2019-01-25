import pandas as pd
import numpy as np
import logging
import logging.config
import time
import datetime

from flowpredicteddao import get_dao
from fbprophet import Prophet

log_filename = "flow_predicted_logging.log"
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a')

class BizFlowPredict(object):
    def __init__(self, days):
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
            forecastflows.append([flow['ds'], flow['yhat'], flow['yhat_lower'], flow['yhat_upper']])
        # 展示预测结果
        #m.plot(forecast).show()
        
        # 预测的成分分析绘图，展示预测中的趋势、周效应和年度效应
        #m.plot_components(forecast).show()
        
        #print(forecast.columns)
        return forecastflows

    def __create_query(self, ids):
        return "SELECT ccircuitid,csbizaveratio,csbizmaxratio,crbizaveratio,\
        crbizmaxratio, cstattime FROM t_biztopflow WHERE ccircuitid in (%s)" % (",".join(str(i) for i in ids))

    def __query_hisflow_biz(self, ids):
        __sql = self.__create_query(ids)
        saveflows = dict()  #
        smaxflows = dict()   #
        raveflows = dict()   #
        rmaxflows = dict()   #
        
        idsaveflows = dict()   # ID关联的每日发送均值流速
        idsmaxflows = dict()   # ID关联的每日发送峰值流速
        idraveflows = dict()   # ID关联的每日接收均值流速
        idrmaxflows = dict()   # ID关联的每日接收均值流速
        ids = set()       # ID集合

        try:
            # 执行SQL语句
            results = get_dao().executeQuery(sql=__sql, param = "None")
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
        except Exception as e:
            logging.error("exception:%s" % e)
            logging.error("Error: unable to fecth data")        
        return ids, idsaveflows, idsmaxflows, idraveflows, idrmaxflows
    
    def __insert_result(self):
        return "insert into t_forecast_biz(emsid,bizid,indicator,yhat,yhat_lower,yhat_upper,predicted_time) values (10000, %s, %s, %s, %s, %s, %s)"

    def __save_foreflow_biz(self, idforeflows, type):
        sqlflows = []
        for id,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((id, type, flow[1], flow[2], flow[3], flow[0].strftime("%Y-%m-%d %H:%M:%S")))
        get_dao().executeInsert(self.__insert_result(), sqlflows)
    
    def predict_biz(self, ids):
        # 根据查询历史流量
        ids, idsaveflows, idsmaxflows, idraveflows, idrmaxflows = self.__query_hisflow_biz(ids)
        
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
    
    predict = BizFlowPredict(days=30)    
    predict.predict_biz(ids)

    logging.info("end biz flow predicted...")

if __name__ == "__main__":
    biz_flow_predicted()