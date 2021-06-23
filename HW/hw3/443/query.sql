ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
USE shabalinaan;

SELECT Logs.response_code, SUM(IF(Users.sex = 'male', 1, 0)) as male, SUM(IF(Users.sex = 'female', 1, 0)) as female
FROM Logs JOIN Users ON Logs.ip=Users.ip
GROUP BY response_code;

