# python3 -m venv venv
# source venv/bin/activate
# pip install pandas datadog_api_client
# 아래 명령어로 실행
# python sync_monitors_with_csv.py

import urllib3
import pandas as pd
from collections import defaultdict
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.monitors_api import MonitorsApi
from datadog_api_client.v1.models import Monitor, MonitorOptions, MonitorThresholds, MonitorUpdateRequest

# Datadog API 키 설정
DATADOG_API_KEY = "<DATADOG_API_KEY>"
DATADOG_APP_KEY = "<DATADOG_APP_KEY>"

# Datadog API 초기화
configuration = Configuration()
configuration.api_key["apiKeyAuth"] = DATADOG_API_KEY
configuration.api_key["appKeyAuth"] = DATADOG_APP_KEY
configuration.verify_ssl = False 
# ㄴ> 테스트 시 TLS 에러 발생으로 인해 False로
# TLS warning 메세지 안보이게 disable
urllib3.disable_warnings()

# CSV 파일 경로
CSV_FILE = "threshold_sample.csv"

# Monitor Message 설정
message = """
{{#is_alert}}
- Account명 : {{host.account_name}}
- Region명 : {{host.region}}
- Host명 : {{host.name_tag}}
- 현재 사용량 : {{eval "round(value, 1)"}}
- Alert 임계치 : {{threshold}}
- 알람 발생 시간 : 
  GMT - {{local_time 'last_triggered_at' 'Africa/Abidjan'}}
  {{#is_match "host.region" "ap-northeast-2"}}KIC-{{local_time 'last_triggered_at' 'Asia/Seoul'}}{{/is_match}}{{#is_match "host.region" "us-west-2"}}AIC-{{local_time 'last_triggered_at' 'America/Los_Angeles'}}{{/is_match}}{{#is_match "host.region" "eu-west-1"}}EIC-{{local_time 'last_triggered_at' 'Europe/Dublin'}}{{/is_match}}{{#is_match "host.region" "ruc"}}RUC-{{local_time 'last_triggered_at' 'Europe/Moscow'}}{{/is_match}}{{^is_match "host.region" "ruc" "ap-northeast-2" "us-west-2" "eu-west-1"}}UTC-{{local_time 'last_triggered_at' 'Etc/UTC'}}{{/is_match}}

{{^is_match "host.account_name" ""}}
  @teams-infra_common
{{else}}
  @teams-{{host.account_name}}_all 
{{/is_match}}

{{/is_alert}}
"""

# 메인이 되는 함수 - CSV 파일의 모니터 내용을 Datadog Monitor 와 Sync 하도록 설정
# csv에 포함된 내용대로 모니터 생성 및 업데이트
# tag값이 automatically_created:true 인 모니터 중에, csv에 포함되지 않은 경우는 삭제됨 (CSV 내용과 동기화를 위해)
def sync_monitors_with_csv(file_path):
    csv_monitors = get_target_monitors_from_csv(file_path)
    if csv_monitors == None:
        print("csv 파일 읽는 중 에러 발생!")
        return

    with ApiClient(configuration) as api_client:
        api_instance = MonitorsApi(api_client)
        existing_monitors = {monitor.name: monitor for monitor in api_instance.list_monitors()}

        # 생성, 업데이트, 삭제를 각각 수행
        handle_creations(api_instance, existing_monitors, csv_monitors)
        handle_updates(api_instance, existing_monitors, csv_monitors)
        handle_deletions(api_instance, existing_monitors, csv_monitors)


# CSV 파일에서 저장할 모니터 설정을 읽고, 정리해서 리턴 
def get_target_monitors_from_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        csv_monitors = create_csv_monitors(df)
    except (FileNotFoundError, KeyError) as e:
        print(f"Error processing CSV: {e}")
        return None
    except KeyError as e:
        print(f"Error: Missing key in CSV file - {e}")
        return None
    except Exception as e:
        print(f"Error unexpected: {e}")
        return None
    
    return csv_monitors

# CSV 데이터에서 모니터 정보를 구성
def create_csv_monitors(df):
    csv_monitors = {}
    grouped_data = defaultdict(lambda: defaultdict(list))
    grouped_data_for_default = defaultdict(lambda: defaultdict(list))

    # 각 행 그룹화
    for _, row in df.iterrows():
        account_name = row["Account_Name"].strip()
        pluginset_name = row["pluginset_name"].strip()
        display_name = row["display_name"].strip()
        critical = int(row["critical"])
        warning = int(row["warning"])
        instance_id = row["Instances_InstanceId"].strip()
        host_name = row["host_name"].strip()

        key = (pluginset_name, display_name, critical, warning)
        grouped_data[key]["instances"].append(instance_id)
        grouped_data[key]["hosts"].append(host_name)

        key_for_default = (pluginset_name, display_name)
        if host_name not in grouped_data_for_default[key_for_default]["hosts"]:
            grouped_data_for_default[key_for_default]["hosts"].append(host_name)
            
    # 모니터 그룹별로 - 스레숄드 설정 및 모니터 이름 설정
    for (pluginset_name, display_name, critical, warning), data in grouped_data.items():
        hosts = " OR ".join([f"name:{host}" for host in data["hosts"]])
        hosts_or = f"({hosts})"

        # tag 에 필요한 정보를 넣을 수 있음. automatically_created:true 를 넣어, 삭제 시 활용
        tags=[f"pluginset_name:{pluginset_name}","automatically_created:true","category:host","default_monitor:false"]

        if pluginset_name == "cpu":
            monitor_name = f"[Infra][Host] CPU 사용률 이상 알람 - {critical}, {warning}"
            query = f"avg(last_5m):100 - avg:system.cpu.idle{{NOT kube_node:* AND {hosts_or}}} by {{host}} > {critical}"
        elif pluginset_name == "memory":
            monitor_name = f"[Infra][Host] Memory 사용률 이상 알람 - {critical}, {warning}"
            query = f"avg(last_5m):avg:system.mem.usable{{NOT kube_node:* AND {hosts_or}}} by {{host}} / avg:system.mem.total{{NOT kube_node:* AND {hosts_or}}} by {{host}} * 100 > {critical}"
        elif pluginset_name == "iowait":
            monitor_name = f"[Infra][Host] IO Wait 이상 알람 - {critical}, {warning}"
            query = f"avg(last_5m):avg:system.cpu.iowait{{NOT kube_node:* AND {hosts_or}}} by {{host}} > {critical}"
        elif pluginset_name == "Disk usage":
            monitor_name = f"[Infra][Host] Disk 사용률 이상 알람 ({display_name}) - {critical}, {warning}"
            critical = critical / 100
            warning = warning / 100
            query = f"avg(last_5m):system.disk.in_use{{NOT kube_node:* AND {hosts_or} AND device:{display_name}}} by {{host}} > {critical}"
        else:
            print(f"Unsupported pluginset_name: {pluginset_name}")
            continue

        if critical == warning:
            thresholds = MonitorThresholds(critical=critical)
        else :
            thresholds = MonitorThresholds(critical=critical, warning=warning)
        options = MonitorOptions(
            thresholds=thresholds,
            # notify_no_data=True,
            # no_data_timeframe=20
        )

        csv_monitors[monitor_name] = {"query": query, "options": options, "tags": tags}

    # default 용도
    for (pluginset_name, display_name), data in grouped_data_for_default.items():        
        hosts = ",!".join([f"name:{host}" for host in data["hosts"]])
        hosts_and = f"!{hosts}"

        # tag 에 필요한 정보를 넣을 수 있음. automatically_created:true 를 넣어, 삭제 시 활용
        tags=[f"pluginset_name:{pluginset_name}","automatically_created:true","category:host","default_monitor:true"]

        if pluginset_name == "cpu":
            critical = 90
            warning = 70
            monitor_name = f"[Infra][Host] CPU 사용률 이상 알람 - Default"
            query = f"avg(last_5m):100 - avg:system.cpu.idle{{!kube_node:*,{hosts_and}}} by {{host}} > {critical}"
        elif pluginset_name == "memory":
            critical = 95
            warning = 85
            monitor_name = f"[Infra][Host] Memory 사용률 이상 알람 - Default"
            query = f"avg(last_5m):avg:system.mem.usable{{!kube_node:*,{hosts_and}}} by {{host}} / avg:system.mem.total{{!kube_node:*,{hosts_and}}} by {{host}} * 100 > {critical}"
        elif pluginset_name == "iowait":
            critical = 60
            warning = 20
            monitor_name = f"[Infra][Host] IO Wait 이상 알람 - Default"
            query = f"avg(last_5m):avg:system.cpu.iowait{{!kube_node:*,{hosts_and}}} by {{host}} > {critical}"
        elif pluginset_name == "Disk usage":
            if display_name == '/' :
                critical = 80
                warning = 70
            else:
                critical = 95
                warning = 85
            monitor_name = f"[Infra][Host] Disk 사용률 이상 알람 ({display_name}) - Default"
            critical = critical / 100
            warning = warning / 100
            query = f"avg(last_5m):system.disk.in_use{{!kube_node:*,{hosts_and},device:{display_name}}} by {{host}} > {critical}"
        else:
            print(f"Unsupported pluginset_name: {pluginset_name}")
            continue

        if critical == warning:
            thresholds = MonitorThresholds(critical=critical)
        else :
            thresholds = MonitorThresholds(critical=critical, warning=warning)
        options = MonitorOptions(
            thresholds=thresholds,
            # notify_no_data=True,
            # no_data_timeframe=20
        )

        csv_monitors[monitor_name] = {"query": query, "options": options, "tags": tags}

    return csv_monitors

# 삭제 - csv 파일에는 없으나 현존 Datadog Monitor 리스트에 있다면, 삭제
def handle_deletions(api_instance, existing_monitors, csv_monitors):
    for monitor_name in set(existing_monitors) - set(csv_monitors):
        # 태그 설정이 automatically_created:true 인 경우만 삭제
        if "automatically_created:true" in existing_monitors[monitor_name].tags:
            try:
                print(f"Deleting monitor: {monitor_name}")
                api_instance.delete_monitor(existing_monitors[monitor_name].id)
            except Exception as e:
                print(f"Error deleting monitor {monitor_name}: {e}")

# 생성 - csv 파일에는 있으나 현존 Datadog Monitor 리스트에 없다면, 생성 
def handle_creations(api_instance, existing_monitors, csv_monitors):
    for monitor_name, data in csv_monitors.items():
        if monitor_name not in existing_monitors:
            try:
                print(f"Creating monitor: {monitor_name}")
                api_instance.create_monitor(
                    Monitor(
                        name=monitor_name,
                        message=message,
                        type="metric alert",
                        query=data["query"],
                        options=data["options"],
                        tags=data["tags"]
                    )
                )
            except Exception as e:
                print(f"Error creating monitor {monitor_name}: {e}")

# 업데이트 - csv 파일과 현존 Datadog Monitor 리스트에 양쪽에 있다면, csv 내용대로 업데이트 
def handle_updates(api_instance, existing_monitors, csv_monitors):
    for monitor_name, data in csv_monitors.items():
        if monitor_name in existing_monitors:
            existing_monitor = existing_monitors[monitor_name]
            try:
                print(f"Updating monitor: {monitor_name}")
                api_instance.update_monitor(
                    existing_monitor.id,
                    MonitorUpdateRequest(
                        name=monitor_name,
                        message=message,
                        type="metric alert",
                        query=data["query"],
                        options=data["options"],
                        tags=data["tags"]
                    )
                )
            except Exception as e:
                print(f"Error updating monitor {monitor_name}: {e}")



sync_monitors_with_csv(CSV_FILE)