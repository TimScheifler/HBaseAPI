```
docker run -d -h <pcName> -p 2181:2181 -p 16020:16020 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 - p 16201:16201 -p 16301:16301 --name hfu-hbase docker.io/dajobe/hbase
```
```
telnet <pcName> 2181
telnet <pcName> 16020
```

```
docker run --rm -it --link "bd6ef84dbe46a82d7495":hbase-docker dajobe/hbase hbase shell
```