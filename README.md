1. nginx отправляет логи по UDP на collector.py

```
log_format repeater '$request_method $request_uri $http_host $status $request_body';
access_log syslog:server=127.0.0.1:9999,facility=local7,tag=nginx,severity=info repeater;
```

2. collector.py сохраняет их в LRU кеш в памяти

```
python collector.py
```

3. reader.py может либо прочитать все записи из кеша collector.py, либо подключиться к collector.py и пописаться на поток логов в прямом эфире

```
python reader.py cache
```

```
python reader.py log
```

4. repeater.py повторяет запросы из логов в несколько потоков

```
python reader.py cache | python repeater.py
```
