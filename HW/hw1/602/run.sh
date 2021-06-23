#!/bin/bash

loc="$(curl -i "http://hadoop2-10.yandex.ru:50070/webhdfs/v1${1}?op=OPEN" | grep "Location:" | sed 's|.* ||' | sed 's/\r$//')"
curl -i "${loc}&length=10"

