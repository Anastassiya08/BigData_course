#!/bin/bash
hdfs fsck $1 -files -blocks -locations | grep -E -o "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" | head -3 | tail -1



# Условие: На вход скрипту подается имя файла, на выходе нужно получить имя сервера или IP-адрес, с которого будет читаться первый блок данных (реплик может быть несколько, засчитываться будет любой из них). Пример:
# $ ./run.sh /data/access_logs/big_log/access.log.2015-12-10
# hadoop2-13.yandex.ru

