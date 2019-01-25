DROP TABLE IF EXISTS t_forecast_biz;
CREATE TABLE t_forecast_biz
(
    emsid bigint NOT NULL COMMENT 'Ems ID'
    bizid bigint NOT NULL COMMENT '电路ID',
    indicator bigint NOT NULL COMMENT '指标类型',
    yhat double NOT NULL COMMENT '预测值',
    yhat_lower double NOT NULL COMMENT '下限',
    yhat_upper double NOT NULL COMMENT '上限',
    predicted_time timestamp NOT NULL COMMENT '时间',
    PRIMARY KEY (emsid,bizid,indicator,predicted_time)
) ENGINE = MyISAM COMMENT='流量预测';

DROP TABLE IF EXISTS t_forecast_trans;
CREATE TABLE t_forecast_trans
(
    emsid bigint NOT NULL COMMENT 'Ems ID'
    bizid bigint NOT NULL COMMENT '传输系统ID',
    indicator bigint NOT NULL COMMENT '指标类型',
    yhat double NOT NULL COMMENT '预测值',
    yhat_lower double NOT NULL COMMENT '下限',
    yhat_upper double NOT NULL COMMENT '上限',
    predicted_time timestamp NOT NULL COMMENT '时间',
    PRIMARY KEY (emsid,bizid,indicator,predicted_time)
) ENGINE = MyISAM COMMENT='流量预测';

DROP TABLE IF EXISTS t_forecast_port;
CREATE TABLE t_forecast_port
(
    emsid bigint NOT NULL COMMENT 'Ems ID'
    neid bigint unsigned NOT NULL COMMENT '网元ID',
    boardid bigint unsigned NOT NULL COMMENT '盘ID',
    portlevel int NOT NULL COMMENT '端口层次',
    portno int unsigned NOT NULL COMMENT '端口号',
    portkey varchar(64) NOT NULL COMMENT '统一端口字符串'
    indicator bigint NOT NULL COMMENT '指标类型',
    yhat double NOT NULL COMMENT '预测值',
    yhat_lower double NOT NULL COMMENT '下限',
    yhat_upper double NOT NULL COMMENT '上限',
    predicted_time timestamp NOT NULL COMMENT '时间',
    PRIMARY KEY (emsid,bizid,indicator,predicted_time)
) ENGINE = MyISAM COMMENT='流量预测';