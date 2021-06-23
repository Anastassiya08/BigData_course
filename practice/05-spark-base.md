### Apache Spark

* Jupyter notebook: `/home/velkerr/msbdp2019/14-15-spark/05-spark-base_nb.ipynb`.
* Открытие ноутбука: `ssh USER@hadoop2.yandex.ru -L 8088:hadoop2-10:8088 -L 18089:hadoop2-10:18089 -L PORT:localhost:PORT`.
* Скопируем ноутбук себе (проделываем на кластере) `cp -r /home/velkerr/msbdp2019/14-15-spark/05-spark-base_nb.ipynb ~/`, а также папку `/home/velkerr/msbdp2019/14-15-spark/images`
* Запуск ноутбука: `PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_PYTHON=/usr/bin/python3 PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip="*" --port=<PORT> --NotebookApp.token="<TOKEN>" --no-browser' pyspark2 --master=yarn --num-executors=2`
* Открыть в браузере: `localhost:PORT`.
