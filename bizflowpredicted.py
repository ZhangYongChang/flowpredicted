import pandas as pd
import logging
import logging.config
from flowpredictedbase import FlowPredictedBase
from flowpredicteddao import get_res_dao
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
        bizIdToSendAvgFlow = dict()
        bizIdToSendMaxFlow = dict()
        bizIdToRecevieAvgFlow = dict()
        bizIdToRecevieMaxFlow = dict()
        for id in bizids:
            bizIdToSendAvgFlow[id] = dict()
            bizIdToSendMaxFlow[id] = dict()
            bizIdToRecevieAvgFlow[id] = dict()
            bizIdToRecevieMaxFlow[id] = dict()
        
        try:
            results = get_res_dao().executeQuery(sql=__sql, param = "None")
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
        return "INSERT INTO t_forecastbizflow(emsid,bizid,indicator,yhat,yhat_lower,yhat_upper,predicted_time) VALUES (10000, %s, %s, %s, %s, %s, %s)"

    def __save_foreflow_biz(self, idforeflows, indicator):
        sqlflows = []
        for id,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((id, indicator, flow[1], flow[2], flow[3], flow[0].strftime("%Y-%m-%d %H:%M:%S")))
        get_res_dao().executeInsert(self.__insert_result(), sqlflows)


def biz_flow_predicted(ids):
    print(ids)
    __sql = """CREATE TABLE t_forecastbizflow(
        emsid BIGINT NOT NULL,
        bizid BIGINT NOT NULL,
        indicator BIGINT NOT NULL,
        yhat DOUBLE NOT NULL,
        yhat_lower DOUBLE NOT NULL,
        yhat_upper DOUBLE NOT NULL,
        predicted_time TIMESTAMP NOT NULL,
        PRIMARY KEY (emsid,bizid,indicator,predicted_time)) ENGINE = MYISAM;"""
    get_res_dao().executeQuery("DROP TABLE IF EXISTS t_forecastbizflow;", param = "None")
    get_res_dao().executeQuery(sql = __sql, param = "None")
    logging.info("begin biz flow predicted...!@###")   
    
    predict = EmsBizFlowPredict(10000, 30)    
    predict.predict_biz_flow(ids)
    logging.info("end biz flow predicted...")
    return 0

if __name__ == "__main__":
    biz_flow_predicted([504399202])
