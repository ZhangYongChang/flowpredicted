import pandas as pd
import logging
import logging.config
from flowpredictedbase import FlowPredictedBase
from flowpredicteddao import get_res_dao
from fbprophet import Prophet

log_filename = "flow_predicted_logging.log"
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a')

class EmsTransFlowPredict(FlowPredictedBase):
    def __init__(self, emsid, days):
        self.emdid = emsid
        FlowPredictedBase.__init__(self, days)

    def predict_trans_flow(self, transIds):
        transIdToSendAvgFlow, transIdToSendMaxFlow, transIdToRecevieAvgFlow, transIdToRecevieMaxFlow = self.__query_hisflow_trans(transIds)
        self.__save_foreflow_trans(self.predict(transIdToSendAvgFlow), 1)
        self.__save_foreflow_trans(self.predict(transIdToSendMaxFlow), 2)
        self.__save_foreflow_trans(self.predict(transIdToRecevieAvgFlow), 3)
        self.__save_foreflow_trans(self.predict(transIdToRecevieMaxFlow), 4)

    def __create_query(self, transIds):
        return "SELECT cobjid,csaverratio,csmaxratio,craverratio,crmaxratio, coccurtime FROM t_transflowstatresult WHERE cobjid in (%s)" % (",".join(str(i) for i in transIds))

    def __query_hisflow_trans(self, transIds):
        __sql = self.__create_query(transIds)
        transIdToSendAvgFlow = dict()   # ID关联的每日发送均值流速
        transIdToSendMaxFlow = dict()   # ID关联的每日发送峰值流速
        transIdToRecevieAvgFlow = dict()   # ID关联的每日接收均值流速
        transIdToRecevieMaxFlow = dict()   # ID关联的每日接收均值流速
        for id in transIds:
            transIdToSendAvgFlow[id] = dict()
            transIdToSendMaxFlow[id] = dict()
            transIdToRecevieAvgFlow[id] = dict()
            transIdToRecevieMaxFlow[id] = dict()
        
        try:
            # 执行SQL语句
            results = get_res_dao().executeQuery(sql=__sql, param = "None")
            for row in results:
                transid = row[0]
                time = row[5].isoformat()
                transIdToSendAvgFlow.get(transid).setdefault(time, row[1])
                transIdToSendMaxFlow.get(transid).setdefault(time, row[2])
                transIdToRecevieAvgFlow.get(transid).setdefault(time, row[3])
                transIdToRecevieMaxFlow.get(transid).setdefault(time, row[4])   
        except Exception as e:
            logging.error("exception:%s" % e)
            logging.error("Error: unable to fecth data")        
        return transIdToSendAvgFlow, transIdToSendMaxFlow, transIdToRecevieAvgFlow, transIdToRecevieMaxFlow
    
    def __insert_result(self):
        return "INSERT INTO t_forecasttransflow(emsid,transid,indicator,yhat,yhat_lower,yhat_upper,predicted_time) VALUES (10000, %s, %s, %s, %s, %s, %s)"

    def __save_foreflow_trans(self, idforeflows, indicator):
        sqlflows = []
        for id,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((id, indicator, flow[1], flow[2], flow[3], flow[0].strftime("%Y-%m-%d %H:%M:%S")))
        get_res_dao().executeInsert(self.__insert_result(), sqlflows)


def trans_flow_predicted(ids):
    '''
    __sql = """CREATE TABLE t_forecasttransflow(
        emsid BIGINT NOT NULL COMMENT 'Ems ID',
        transid BIGINT NOT NULL COMMENT '传输系统ID',
        indicator INT NOT NULL COMMENT '指标类型',
        yhat DOUBLE NOT NULL COMMENT '预测值',
        yhat_lower DOUBLE NOT NULL COMMENT '下限',
        yhat_upper DOUBLE NOT NULL COMMENT '上限',
        predicted_time TIMESTAMP NOT NULL COMMENT '时间',
        PRIMARY KEY (emsid,transid,indicator,predicted_time)) ENGINE = MYISAM COMMENT='流量预测';"""
    get_res_dao().executeQuery("DROP TABLE IF EXISTS t_forecasttransflow;", param = "None")
    get_res_dao().executeQuery(sql = __sql, param = "None")
    '''
    logging.info("begin trans flow predicted...")

    print(ids)
    ids = [603979786,603979787]
    
    predict = EmsTransFlowPredict(10000, 30)    
    predict.predict_trans_flow(ids)

    logging.info("end trans flow predicted...")
    return 0

if __name__ == "__main__":
    trans_flow_predicted()