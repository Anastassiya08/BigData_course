ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
USE shabalinaan;

SELECT table0.code as code, COUNT(table0.male) as male, COUNT(table0.female) as female
FROM (SELECT Logs.response_code as code, IF(Users.sex = 'male', 1, 0) as male, IF(Users.sex = 'female', 1, 0) as female
FROM Logs TABLESAMPLE(10 PERCENT) as l JOIN Users TABLESAMPLE(10 PERCENT) as u ON l.ip=u.ip) as table0
GROUP BY code;
