ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE shabalinaan;

SELECT TRANSFORM(ip, day, request, size, response_code, info)
USING "sed 's/Safari/Chrome/'" AS ip, day, request, size, response_code, info
FROM Logs
LIMIT 10;
