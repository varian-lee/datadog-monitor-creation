# datadog-monitor-creation

csv 파일을 읽고 Datadog Monitor와 동기화하는 스크립트

```
# 아래와 같이 준비
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install pandas datadog_api_client

# 아래 명령어로 스크립트 실행
$ python sync_monitors_with_csv.py
```
