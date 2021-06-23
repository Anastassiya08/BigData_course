ADD JAR Motivator/jar/Motivator.jar;

USE shabalinaan;

create temporary function motivate as 'com.hobod.MotivateUDF';

SELECT motivate(Users.age)
FROM Logs JOIN Users ON Logs.ip=Users.ip
LIMIT 10;
