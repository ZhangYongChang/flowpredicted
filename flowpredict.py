import threading
import logging
import bizflowpredicted
import linkflowpredicted
import portflowpredicted
import transflowpredicted

class FlowPredictThread(threading.Thread):
    def __init__(self, func, ids, batch):
        threading.Thread.__init__(self)
        self.func = func
        self.ids = ids
        self.batch = batch

    def run(self):
        count = len(self.ids)
        if count == 0:
            return
        
        begin = 0    
        end = begin + self.batch
        tmpid = self.ids[begin:end]
        self.func(tmpid)
        while end < count:
            begin = end
            end = begin + self.batch
            tmpid = self.ids[begin:end]
            self.func(tmpid)

def flowpredict(bizid, topolinkid, transysid, neid):
    bizflowpredictedthread = FlowPredictThread(bizflowpredicted.biz_flow_predicted, bizid, 4000)
    portflowpredictedthread = FlowPredictThread(portflowpredicted.port_flow_predicted, neid, 200)
    linkflowpredictedthread = FlowPredictThread(linkflowpredicted.link_flow_predicted, topolinkid, 4000)
    transflowpredictedthread = FlowPredictThread(transflowpredicted.trans_flow_predicted, transysid, 4000)
    bizflowpredictedthread.start()
    portflowpredictedthread.start()
    linkflowpredictedthread.start()
    transflowpredictedthread.start()
    bizflowpredictedthread.join()
    portflowpredictedthread.join()
    linkflowpredictedthread.join()
    transflowpredictedthread.join()

if __name__ == "__main__":
    flowpredict([504399202], [504399202], [504399202], [504399202])
