
```bash
nohup yarn-session.sh -d -nm onework -s 2 -jm 1024 -tm 1536 &
```

```bash
cp /home/kangjiao/gitlab/onework/onework-function/target/onework-function-1.0.0-SNAPSHOT.jar ~/jars/
```

清除旧的状态
```bash
redis-cli -h bdnode5 -n 15 keys departure* | xargs redis-cli -h bdnode5 -n 15 del
```