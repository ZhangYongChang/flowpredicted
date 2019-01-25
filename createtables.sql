DROP TABLE IF EXISTS t_forecast_biz;
CREATE TABLE t_forecast_biz
(
    emsid BIGINT NOT NULL COMMENT 'Ems ID',
    bizid BIGINT NOT NULL COMMENT '电路ID',
    indicator BIGINT NOT NULL COMMENT '指标类型',
    yhat DOUBLE NOT NULL COMMENT '预测值',
    yhat_lower DOUBLE NOT NULL COMMENT '下限',
    yhat_upper DOUBLE NOT NULL COMMENT '上限',
    predicted_time TIMESTAMP NOT NULL COMMENT '时间',
    PRIMARY KEY (emsid,bizid,indicator,predicted_time)
) ENGINE = MYISAM COMMENT='流量预测';

DROP TABLE IF EXISTS t_forecast_trans;
CREATE TABLE t_forecast_trans
(
    emsid BIGINT NOT NULL COMMENT 'Ems ID',
    bizid BIGINT NOT NULL COMMENT '传输系统ID',
    indicator BIGINT NOT NULL COMMENT '指标类型',
    yhat DOUBLE NOT NULL COMMENT '预测值',
    yhat_lower DOUBLE NOT NULL COMMENT '下限',
    yhat_upper DOUBLE NOT NULL COMMENT '上限',
    predicted_time TIMESTAMP NOT NULL COMMENT '时间',
    PRIMARY KEY (emsid,bizid,indicator,predicted_time)
) ENGINE = MYISAM COMMENT='流量预测';

DROP TABLE IF EXISTS t_forecast_port;
CREATE TABLE t_forecast_port
(
    emsid BIGINT NOT NULL COMMENT 'Ems ID',
    neid BIGINT UNSIGNED NOT NULL COMMENT '网元ID',
    boardid BIGINT UNSIGNED NOT NULL COMMENT '盘ID',
    portlevel INT NOT NULL COMMENT '端口层次',
    portno INT UNSIGNED NOT NULL COMMENT '端口号',
    portkey VARCHAR(64) NOT NULL COMMENT '统一端口字符串'
    indicator BIGINT NOT NULL COMMENT '指标类型',
    yhat DOUBLE NOT NULL COMMENT '预测值',
    yhat_lower DOUBLE NOT NULL COMMENT '下限',
    yhat_upper DOUBLE NOT NULL COMMENT '上限',
    predicted_time TIMESTAMP NOT NULL COMMENT '时间',
    PRIMARY KEY (emsid,bizid,indicator,predicted_time)
) ENGINE = MYISAM COMMENT='流量预测';