ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
USE shabalinaan;

SELECT info, COUNT(ip) as cnt
FROM Logs 
GROUP BY info
ORDER BY cnt DESC;
