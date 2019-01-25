import pandas as pd
import logging
import logging.config
from fbprophet import Prophet
from matplotlib import pyplot as plt

class FlowPredictedBase(object):
    def __init__(self, days):
        self.__days = days

    def __predict(self, dayflows):
        df = pd.DataFrame(data=dayflows,columns=["ds","y"])
        m = Prophet()
        m.fit(df)
        future = m.make_future_dataframe(periods=self.__days)
        forecast = m.predict(future)
        m.plot(forecast).savefig("test.png")
        forecastflows = []
        for index, flow in forecast.iterrows():
            forecastflows.append([flow['ds'], flow['yhat'], flow['yhat_lower'], flow['yhat_upper']])
        return forecastflows

    def predict(self, objToDayFlow):
        sortedObjDayFlow = dict()
        for objId, dayFlows in objToDayFlow.items():
            dayFlows = sorted(dayFlows.items(), key=lambda d:d[0])
            sortedObjDayFlow[objId] = dayFlows
        objIdToForecastFlows = dict()
        for objId, dayFlows in sortedObjDayFlow.items():
            forecastflows = self.__predict(dayFlows)
            objIdToForecastFlows[objId] = forecastflows
        return objIdToForecastFlows
        