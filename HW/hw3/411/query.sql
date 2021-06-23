ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

USE shabalinaan;

DROP TABLE IF EXISTS Logs0;

CREATE EXTERNAL TABLE Logs0 (
    ip STRING,
    day INT,
    request STRING,
    size INT,
    response_code INT,
    info STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+)\\t\\t\\t(\\d{8})\\S*\\t(\\S+)\\t(\\d+)\\t(\\d{3})\\t(\\S+)\\s+.*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

SELECT * FROM Logs0 LIMIT 10;

DROP TABLE IF EXISTS Users;

CREATE EXTERNAL TABLE Users (
    ip STRING,
    browser STRING,
    sex STRING,
    age INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+)\\t(\\S+)\\t(\\S+)\\t(\\d+).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';

SELECT * FROM Users LIMIT 10;

DROP TABLE IF EXISTS IPRegions;

CREATE EXTERNAL TABLE IPRegions (
    ip STRING,
    region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+)\\t(\\S+.*)$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

SELECT * FROM IPRegions LIMIT 10;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=200;
SET hive.exec.max.dynamic.partitions.pernode=20000;
DROP TABLE IF EXISTS Logs;

CREATE EXTERNAL TABLE Logs (
    ip STRING,
    request STRING,
    size INT,
    response_code INT,
    info STRING
)
PARTITIONED BY (day INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION (day)
SELECT Logs0.ip as ip, Logs0.request as request, Logs0.size as size, Logs0.response_code as response_code, Logs0.info as info, Logs0.day as day FROM Logs0;

SELECT * FROM Logs LIMIT 10;

DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+)\\t(\\S+)$'
)
STORED AS TEXTFILE
LOCATION '/data/subnets/variant3';

SELECT * FROM Subnets LIMIT 10;




