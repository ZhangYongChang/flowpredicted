import pandas as pd
import logging
import logging.config
from flowpredictedbase import FlowPredictedBase
from flowpredicteddao import get_dao
from fbprophet import Prophet

log_filename = "flow_predicted_logging.log"
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a')

class EmsBizFlowPredict(FlowPredictedBase):
    def __init__(self, emsid, days):
        self.emdid = emsid
        FlowPredictedBase.__init__(self, days)

    def predict_biz_flow(self, bizIds):
        bizIdToSendAvgFlow, bizIdToSendMaxFlow, bizIdToRecevieAvgFlow, bizIdToRecevieMaxFlow = self.__query_hisflow_biz(bizIds)
        self.__save_foreflow_biz(self.predict(bizIdToSendAvgFlow, 1), 1)
        self.__save_foreflow_biz(self.predict(bizIdToSendMaxFlow, 2), 2)
        self.__save_foreflow_biz(self.predict(bizIdToRecevieAvgFlow, 3), 3)
        self.__save_foreflow_biz(self.predict(bizIdToRecevieMaxFlow, 4), 4)

    def __create_query(self, bizids):
        return "SELECT ccircuitid,csbizaveratio,csbizmaxratio,crbizaveratio,crbizmaxratio, cstattime FROM t_biztopflow WHERE ccircuitid in (%s)" % (",".join(str(i) for i in bizids))

    def __query_hisflow_biz(self, bizids):
        __sql = self.__create_query(bizids)
        bizIdToSendAvgFlow = dict()   # ID关联的每日发送均值流速
        bizIdToSendMaxFlow = dict()   # ID关联的每日发送峰值流速
        bizIdToRecevieAvgFlow = dict()   # ID关联的每日接收均值流速
        bizIdToRecevieMaxFlow = dict()   # ID关联的每日接收均值流速
        for id in bizids:
            bizIdToSendAvgFlow[id] = dict()
            bizIdToSendMaxFlow[id] = dict()
            bizIdToRecevieAvgFlow[id] = dict()
            bizIdToRecevieMaxFlow[id] = dict()
        
        try:
            # 执行SQL语句
            results = get_dao().executeQuery(sql=__sql, param = "None")
            for row in results:
                bizid = row[0]
                time = row[5].isoformat()
                bizIdToSendAvgFlow.get(bizid).setdefault(time, row[1])
                bizIdToSendMaxFlow.get(bizid).setdefault(time, row[2])
                bizIdToRecevieAvgFlow.get(bizid).setdefault(time, row[3])
                bizIdToRecevieMaxFlow.get(bizid).setdefault(time, row[4])   
        except Exception as e:
            logging.error("exception:%s" % e)
            logging.error("Error: unable to fecth data")        
        return bizIdToSendAvgFlow, bizIdToSendMaxFlow, bizIdToRecevieAvgFlow, bizIdToRecevieMaxFlow
    
    def __insert_result(self):
        return "INSERT INTO t_forecast_biz(emsid,bizid,indicator,yhat,yhat_lower,yhat_upper,predicted_time) VALUES (10000, %s, %s, %s, %s, %s, %s)"

    def __save_foreflow_biz(self, idforeflows, indicator):
        sqlflows = []
        for id,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((id, indicator, flow[1], flow[2], flow[3], flow[0].strftime("%Y-%m-%d %H:%M:%S")))
        get_dao().executeInsert(self.__insert_result(), sqlflows)


def biz_flow_predicted():
    __sql = """CREATE TABLE t_forecast_biz(
        emsid BIGINT NOT NULL COMMENT 'Ems ID',
        bizid BIGINT NOT NULL COMMENT '电路ID',
        indicator BIGINT NOT NULL COMMENT '指标类型',
        yhat DOUBLE NOT NULL COMMENT '预测值',
        yhat_lower DOUBLE NOT NULL COMMENT '下限',
        yhat_upper DOUBLE NOT NULL COMMENT '上限',
        predicted_time TIMESTAMP NOT NULL COMMENT '时间',
        PRIMARY KEY (emsid,bizid,indicator,predicted_time)) ENGINE = MYISAM COMMENT='流量预测';"""
    get_dao().executeQuery("DROP TABLE IF EXISTS t_forecast_biz;", param = "None")
    get_dao().executeQuery(sql = __sql, param = "None")
    logging.info("begin biz flow predicted...")

    ids = [504365114,504365122,504384840,504383881,504403704,504405833]
    #ids = [504384840]
    
    predict = EmsBizFlowPredict(10000, 30)    
    predict.predict_biz_flow(ids)

    logging.info("end biz flow predicted...")

if __name__ == "__main__":
    biz_flow_predicted()