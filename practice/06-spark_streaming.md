#### Подсчёт непустых строк. Источник - netcat.

1. Запускаем tmux c 2мя окнами (или просто 2 терминала).

* в одном окне запускаем `nc -lk 127.0.0.1 -p <PORT>`. Порт берём уникальный.
* в другом - через `spark2-submit` запускаем код ниже.

```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')  # режим запуска локальный 
ssc = StreamingContext(sc, batchDuration=10)  # каждые 10 с. формируем батч из введенного текста
dstream = ssc.socketTextStream(hostname='localhost', port=9999) # подключаемся к порту **<PORT>** (или к другому, на кот. запущен netcat)

result = dstream.filter(bool).count()  # считаем непустые строки
result.pprint()  # выводим результат по каждому батчу

ssc.start()
ssc.awaitTermination() # ждём ^C
```

2. В том терминале, где запущен netcat, вводим строки текста. Некоторые из них - пустые.
3. Наблюдаем за тем, как считаются строки:

```
-------------------------------------------
Time: 2019-04-04 11:13:00
-------------------------------------------
6
```
6 - число непустых строк.

#### Подсчёт непустых строк. Источник - HDFS.

Работать будем с библиотекой [hdfscli](https://hdfscli.readthedocs.io/en/latest/). Для того, чтоб библиотека заработала, сделайте следующее:

1) зайдите на клиент и создайте файл `~/.hdfscli.cfg`.
2) Добавьте такой конфиг:
```
[global]
default.alias = default

[default.alias]
url = http://hadoop2-10:50070
user = <USER>
```
Вместо <USER> напишите своего пользователя (выполните `whoami`).

Теперь cоздаём приложение.

```python
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from hdfs import Config
import subprocess

client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")
sc = SparkContext(master='yarn')  # т.к. работаем с HDFS, можем запустить Spark в распределённом режиме

# Эмулируем реальную жизнь, когда данные поступают частями с периодичностью
DATA_PATH = "/data/course4/wiki/en_articles_batches"
batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)]  # формируем батчи из файлов датасета
BATCH_TIMEOUT = 5 # раз в 5 с. посылаем батчи в виде RDD
ssc = StreamingContext(sc, BATCH_TIMEOUT)

dstream = ssc.queueStream(rdds=batches)
result = dstream.filter(bool).count()
result.pprint()

ssc.start()
ssc.awaitTermination()  # ждём SIGINT (^C)
```

Результат:
```
-------------------------------------------
Time: 2019-04-04 11:20:14
-------------------------------------------
4256

-------------------------------------------
Time: 2019-04-04 11:20:24
-------------------------------------------
4238
```

Когда данные заканчиваются, начнут выводиться 0 в результатах. Можем прерывать.

#### Подсчёт кол-ва слов (WordCount).

```
def print_rdd(rdd):
    for row in rdd.take(10):
        print('{}\t{}'.format(*row))

result = dstream \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .foreachRDD(lambda rdd: print_rdd(rdd.sortBy(lambda x: -x[1])))
```

Ждём когда перестанет выводить числа и прерываем.

Какие недостатки у решения?

#### Statefult-подсчёт суммы чисел 

> Дана последовательность чисел `0, 1, 2, 3, 4, ...`, поступающих в реальном времени. Подсчитайте сумму последовательности.

```python
import os
from time import sleep
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='yarn')

NUM_BATCHES = 10  # длина последовательности
batches = [sc.parallelize([num]) for num in range(NUM_BATCHES)]

BATCH_TIMEOUT = 5 # период генерации RDD
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)

def print_always(rdd):
    print("Result: {}".format(rdd.collect()[0]))

def aggregator(values, old):
    return (old or 0) + sum(values)

# `updateStateByKey` needs key-value structue so you need to specify fictive key "res"
# and then remove it after aggregation

dstream.map(lambda num: ('res', num)) \
   .updateStateByKey(aggregator) \
   .map(lambda x: x[1]) \
   .foreachRDD(print_always)

# Присваиваем уникальное имя checkpoint'у
ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))  # Сохраняем накопления
ssc.start()
ssc.awaitTermination()  # ждём SIGINT (^C)
```

#### WordCount с накоплениями (Stateful)

Добавляем Stateful-подход в WordCount. Нужно:

* добавить updateStateByKey(func)
* изменить print_always()

#### Учим приложение останавливаться автоматически когда данные закончились

1) До начала обработки проверяем, приходят ли ещё данные.

```python
finished = False

def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True

dstream.foreachRDD(set_ending_flag)
```

2) Вместо `ssc.awaitTermination()` останавливаем контекст только когда данные закончились.

```python
ssc.start()
while not finished:
    time.sleep(0.001)
ssc.stop()
```

#### Исходники

/home/velkerr/msbdp2019/16-spark-streaming

#### Дополнительные примеры

[Оф. репозиторий Spark](https://github.com/apache/spark/tree/master/examples/src/main/python/streaming)


