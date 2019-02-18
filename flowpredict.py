import threading
import logging
import bizflowpredicted
import portflowpredicted

class FlowPredictThread(threading.Thread):
    def __init__(self, name, func, ids, batch):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.ids = ids
        self.batch = batch

    def run(self):
        logging.info("flow preidct begin..." % self.name)

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

        logging.info("flow preidct end..." % self.name)


def flowpredict(bizid, topolinkid, transysid, neid):
    bizflowpredictedthread = FlowPredictThread("bizflowpredicted", bizflowpredicted.biz_flow_predicted, bizid, 4000)
    thread2 = FlowPredictThread("", portflowpredicted.)
    bizflowpredictedthread.start()
    thread2.start()
    bizflowpredictedthread.join()
    thread2.join()
