import pandas as pd
import logging
import logging.config
from flowpredictedbase import FlowPredictedBase
from flowpredicteddao import get_res_dao
from fbprophet import Prophet

log_filename = "flow_predicted_logging.log"
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a')

class Port:
    def __init__(self):
        self.emsid = 0
        self.neid = 0
        self.boardid = 0
        self.portlevel = 0
        self.portno = 0
        self.portstr = ""
    
    def __init__(self, emsid, neid, boardid, portlevel, portno, portstr):
        self.emsid = emsid
        self.neid = neid
        self.boardid = boardid
        self.portlevel = portlevel
        self.portno = portno
        self.portstr = portstr

    def __hash__(self):
        str = "(%d,%d,%d,%d,%d,'%s')" % (self.emsid, self.neid, self.boardid, self.portlevel, self.portno, self.portstr)
        return hash(str)
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.__dict__ == self.__dict__
        return False

    def getstring(self):
        return "(%s,%s,%s,%s,%s,'%s')" % (self.emsid, self.neid, self.boardid, self.portlevel, self.portno, self.portstr)
        
class EmsPortFlowPredict(FlowPredictedBase):
    def __init__(self, emsid, days):
        self.emsid = emsid
        FlowPredictedBase.__init__(self, days)

    def predict_port_flow(self, ports):
        portToSendAvgFlow, portToSendMaxFlow, portToRecevieAvgFlow, portToRecevieMaxFlow = self.__query_hisflow_port(ports)
        self.__save_foreflow_port(self.predict(portToSendAvgFlow), 1)
        self.__save_foreflow_port(self.predict(portToSendMaxFlow), 2)
        self.__save_foreflow_port(self.predict(portToRecevieAvgFlow), 3)
        self.__save_foreflow_port(self.predict(portToRecevieMaxFlow), 4)

    def __create_query(self, month, neids):
        return "SELECT cneid,cboardid,cportlevel,cportno,cportkey,csaveratio,cspeakratio,craveratio,crpeakratio,ctime FROM t_portbusyflow_%s WHERE cneid in (%s)" % (month, (",".join(str(i) for i in neids)))

    def __getportdic(self, port, portToFlow):
        if port not in portToFlow:
            portToFlow[port] = dict() 
        return portToFlow[port]

    def __query_hisflow_port(self, neids):

        portToSendAvgFlow = dict()   # ID关联的每日发送均值流速
        portToSendMaxFlow = dict()   # ID关联的每日发送峰值流速
        portToRecevieAvgFlow = dict()   # ID关联的每日接收均值流速
        portToRecevieMaxFlow = dict()   # ID关联的每日接收均值流速
        
        for month in range(1, 12):
            __sql = self.__create_query(month, neids)
            
            try:
                # 执行SQL语句
                results = get_res_dao().executeQuery(sql=__sql, param = "None")
                for row in results:
                    port = Port(self.emsid, row[0], row[1], row[2], row[3], row[4])
                    time = row[9].isoformat()
                    self.__getportdic(port, portToSendAvgFlow).setdefault(time, row[5])
                    self.__getportdic(port, portToSendMaxFlow).setdefault(time, row[6])
                    self.__getportdic(port, portToRecevieAvgFlow).setdefault(time, row[7])
                    self.__getportdic(port, portToRecevieMaxFlow).setdefault(time, row[8]) 
            except Exception as e:
                logging.error("exception:%s" % e)
                logging.error("Error: unable to fecth data")
        
        return portToSendAvgFlow, portToSendMaxFlow, portToRecevieAvgFlow, portToRecevieMaxFlow
     
    def __insert_result(self):
        return "INSERT INTO t_forecastportflow(emsid,neid,boardid,portlevel,portno,portkey,indicator,yhat,yhat_lower,yhat_upper,predicted_time) VALUES (10000, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    def __save_foreflow_port(self, idforeflows, indicator):
        sqlflows = []
        for port,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((port.neid, port.boardid, port.portlevel, port.portno, port.portstr, indicator, flow[1], flow[2], flow[3], flow[0].strftime("%Y-%m-%d %H:%M:%S")))
        get_res_dao().executeInsert(self.__insert_result(), sqlflows)


def port_flow_predicted(ids):
    '''
    __sql = """CREATE TABLE t_forecastportflow(
        emsid BIGINT NOT NULL COMMENT 'Ems ID',
        bizid BIGINT NOT NULL COMMENT '电路ID',
        indicator BIGINT NOT NULL COMMENT '指标类型',
        yhat DOUBLE NOT NULL COMMENT '预测值',
        yhat_lower DOUBLE NOT NULL COMMENT '下限',
        yhat_upper DOUBLE NOT NULL COMMENT '上限',
        predicted_time TIMESTAMP NOT NULL COMMENT '时间',
        PRIMARY KEY (emsid,bizid,indicator,predicted_time)) ENGINE = MYISAM COMMENT='流量预测';"""
    get_res_dao().executeQuery("DROP TABLE IF EXISTS t_forecastportflow;", param = "None")
    get_res_dao().executeQuery(sql = __sql, param = "None")
    '''
    logging.info("begin port flow predicted...")

    print(ids)
    predict = EmsPortFlowPredict(10000, 30)    
    predict.predict_port_flow(ids)

    logging.info("end port flow predicted...")
    return 0

if __name__ == "__main__":
    port_flow_predicted([134217731])