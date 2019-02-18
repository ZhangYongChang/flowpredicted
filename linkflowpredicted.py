import pandas as pd
import logging
import logging.config
from portflowpredicted import Port
from flowpredictedbase import FlowPredictedBase
from flowpredicteddao import get_res_dao
from flowpredicteddao import get_topo_dao
from fbprophet import Prophet

log_filename = "flow_predicted_logging.log"
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a')

class EmsLinkFlowPredict(FlowPredictedBase):
    def __init__(self, emsid, days):
        self.emsid = emsid
        FlowPredictedBase.__init__(self, days)

    def predict_link_flow(self, linkIds):
        linkIdToSendAvgFlow, linkIdToSendMaxFlow, linkIdToRecevieAvgFlow, linkIdToRecevieMaxFlow = self.__query_hisflow_link(linkIds)
        self.__save_foreflow_link(self.predict(linkIdToSendAvgFlow), 1)
        self.__save_foreflow_link(self.predict(linkIdToSendMaxFlow), 2)
        self.__save_foreflow_link(self.predict(linkIdToRecevieAvgFlow), 3)
        self.__save_foreflow_link(self.predict(linkIdToRecevieMaxFlow), 4)

    def __create_query(self, month, ports):
        return "SELECT cneid,cboardid,cportlevel,cportno,cportkey,csaveratio,cspeakratio,craveratio,crpeakratio,ctime FROM t_portbusyflow_%s WHERE (cemsid,cneid,cboardid,cportlevel,cportno,cportkey) in (%s)" % (month, (",".join(port.getstring() for port in ports)))
               
    def __query_hisflow_port(self, ports):
        portToFlow = dict()
        for port in ports:
            portToFlow[port] = dict()
        
        for month in range(1, 12):
            __sql = self.__create_query(month, ports)
            try:
                # 执行SQL语句
                results = get_res_dao().executeQuery(sql=__sql, param = "None")
                for row in results:
                    port = Port(self.emsid, row[0], row[1], row[2], row[3], row[4])
                    time = row[9].isoformat()
                    portToFlow.get(port).setdefault(time, [row[5], row[6], row[7], row[8]])  
            except Exception as e:
                logging.error("exception:%s" % e)
                logging.error("Error: unable to fecth data")        
        return portToFlow
    
    def __create_topolink_query(self, linkids):
        return "SELECT emstopolinkid,srcemsid,srcneid,srcboardid,srcportlevel,srcportno,srcportkeystr,dstneid,dstboardid,dstportlevel,dstportno,dstportkeystr from topolink where emstopolinkid in (%s)" % (",".join(str(i) for i in linkids))
               
    def __query_linktoport(self, linkids):
        __sql = self.__create_topolink_query(linkids) 
        linkIdToPort = dict()
        ports = []
        
        try:
            # 执行SQL语句
            results = get_topo_dao().executeQuery(sql=__sql, param = "None")
            for row in results:
                srcport = Port(row[1], row[2], row[3], row[4], row[5], row[6])
                ports.append(srcport)
                dstport = Port(row[1], row[7], row[8], row[9], row[10], row[11])
                ports.append(dstport)               
                linkIdToPort[row[0]] = (srcport, dstport)
                
        except Exception as e:
            logging.error("exception:%s" % e)
            logging.error("Error: unable to fecth data")
            
        return linkIdToPort, ports
               
    def __query_hisflow_link(self, linkids):
        linkIdToPort, ports = self.__query_linktoport(linkids)       
        portToFlow = self.__query_hisflow_port(ports)
        print(linkIdToPort)
        
        linkIdToSendAvgFlow = dict()   # ID关联的每日发送均值流速
        linkIdToSendMaxFlow = dict()   # ID关联的每日发送峰值流速
        linkIdToRecevieAvgFlow = dict()   # ID关联的每日接收均值流速
        linkIdToRecevieMaxFlow = dict()   # ID关联的每日接收均值流速
        
        for id in linkids:
            linkIdToSendAvgFlow[id] = dict()
            linkIdToSendMaxFlow[id] = dict()
            linkIdToRecevieAvgFlow[id] = dict()
            linkIdToRecevieMaxFlow[id] = dict()
        
        for id,srcdstport in linkIdToPort.items():
            print(srcdstport)
            srcport = srcdstport[0]
            dstport = srcdstport[1]
            print(srcport.getstring())
            # 取源端口收为收方向
            if srcport in portToFlow:
                for time,flow in portToFlow[srcport].items():
                    linkIdToRecevieAvgFlow[id][time] = flow[2]
                    linkIdToRecevieMaxFlow[id][time] = flow[3]
            # 取宿端口收为发方向
            if dstport in portToFlow:
                for time,flow in portToFlow[dstport].items():
                    linkIdToSendAvgFlow[id][time] = flow[2]
                    linkIdToSendMaxFlow[id][time] = flow[3]
        print(linkIdToRecevieAvgFlow)
        return linkIdToSendAvgFlow, linkIdToSendMaxFlow, linkIdToRecevieAvgFlow, linkIdToRecevieMaxFlow
    
    def __insert_result(self):
        return "INSERT INTO t_forecastlinkflow(emsid,linkid,indicator,yhat,yhat_lower,yhat_upper,predicted_time) VALUES (10000, %s, %s, %s, %s, %s, %s)"

    def __save_foreflow_link(self, idforeflows, indicator):
        sqlflows = []
        for id,flows in idforeflows.items():
            for flow in flows:
                sqlflows.append((id, indicator, flow[1], flow[2], flow[3], flow[0].strftime("%Y-%m-%d %H:%M:%S")))
        get_res_dao().executeInsert(self.__insert_result(), sqlflows)


def link_flow_predicted(ids):
    '''
    __sql = """CREATE TABLE t_forecastlinkflow(
        emsid BIGINT NOT NULL COMMENT 'Ems ID',
        linkid BIGINT NOT NULL COMMENT '电路ID',
        indicator BIGINT NOT NULL COMMENT '指标类型',
        yhat DOUBLE NOT NULL COMMENT '预测值',
        yhat_lower DOUBLE NOT NULL COMMENT '下限',
        yhat_upper DOUBLE NOT NULL COMMENT '上限',
        predicted_time TIMESTAMP NOT NULL COMMENT '时间',
        PRIMARY KEY (emsid,linkid,indicator,predicted_time)) ENGINE = MYISAM COMMENT='流量预测';"""
    get_res_dao().executeQuery("DROP TABLE IF EXISTS t_forecastlinkflow;", param = "None")
    get_res_dao().executeQuery(sql = __sql, param = "None")
    '''
    logging.info("begin link flow predicted...")

    print(ids)
    ids = [570425351,570425352]
    
    predict = EmsLinkFlowPredict(10000, 30)    
    predict.predict_link_flow(ids)

    logging.info("end link flow predicted...")
    return 0

if __name__ == "__main__":
    link_flow_predicted()