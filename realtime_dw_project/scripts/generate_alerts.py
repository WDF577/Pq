"""实时异常告警生成器 — 基于 ClickHouse 查询结果生成告警"""
import argparse
import json
import urllib.request
import urllib.parse
import base64
import time
from datetime import datetime, timedelta


def query_clickhouse(url, user, password, query):
    """执行 ClickHouse 查询并返回结果"""
    params = urllib.parse.urlencode({"query": query, "default_format": "JSONEachRow"})
    auth = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        f"{url}/?{params}",
        headers={"Authorization": f"Basic {auth}"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return [json.loads(line) for line in resp.read().decode("utf-8").strip().split("\n") if line]


def insert_alert(url, user, password, alert):
    """写入一条告警到 ClickHouse"""
    query = urllib.parse.urlencode({"query": "INSERT INTO ads_realtime_alert FORMAT JSONEachRow"})
    data = (json.dumps(alert, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    auth = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        f"{url}/?{query}",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth}",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def parse_args():
    parser = argparse.ArgumentParser(description="Generate real-time alerts from ClickHouse metrics.")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="clickhouse")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    return parser.parse_args()


def main():
    args = parse_args()
    ch = {
        "url": args.clickhouse_url,
        "user": args.clickhouse_user,
        "password": args.clickhouse_password,
    }

    print("Real-time alert generator started.")
    while True:
        try:
            # 查询最近 10 个窗口的概览数据
            rows = query_clickhouse(
                ch["url"], ch["user"], ch["password"],
                "SELECT * FROM ads_realtime_overview ORDER BY window_start DESC LIMIT 10",
            )

            if not rows:
                print("  no overview data yet, waiting...")
            else:
                latest = rows[0]
                alerts = []

                # 告警 1: 支付金额为 0
                if float(latest.get("pay_amount", 0)) == 0:
                    alerts.append({
                        "window_start": latest["window_start"],
                        "window_end": latest["window_end"],
                        "alert_type": "pay_amount_zero",
                        "alert_level": "WARNING",
                        "alert_message": f"支付金额为 0，窗口: {latest['window_start']} ~ {latest['window_end']}",
                        "metric_name": "pay_amount",
                        "metric_value": 0.0,
                    })

                # 告警 2: 支付人数为 0
                if int(latest.get("pay_users", 0)) == 0:
                    alerts.append({
                        "window_start": latest["window_start"],
                        "window_end": latest["window_end"],
                        "alert_type": "pay_users_zero",
                        "alert_level": "WARNING",
                        "alert_message": f"支付人数为 0，窗口: {latest['window_start']} ~ {latest['window_end']}",
                        "metric_name": "pay_users",
                        "metric_value": 0.0,
                    })

                # 告警 3: 支付金额骤降（当前窗口 < 最近5窗口平均值 × 50%）
                if len(rows) >= 5:
                    recent_amounts = [float(r.get("pay_amount", 0)) for r in rows[1:6]]
                    avg_amount = sum(recent_amounts) / len(recent_amounts)
                    cur_amount = float(latest.get("pay_amount", 0))
                    if avg_amount > 0 and cur_amount < avg_amount * 0.5:
                        alerts.append({
                            "window_start": latest["window_start"],
                            "window_end": latest["window_end"],
                            "alert_type": "pay_amount_drop",
                            "alert_level": "CRITICAL",
                            "alert_message": f"支付金额骤降: {cur_amount:.2f} < 均值 {avg_amount:.2f} × 50%",
                            "metric_name": "pay_amount",
                            "metric_value": cur_amount,
                        })

                # 告警 4: 访客到支付用户转化率异常（UV → pay_users < 1%）
                uv = int(latest.get("uv", 0))
                pay_users_val = int(latest.get("pay_users", 0))
                if uv > 100 and pay_users_val / uv < 0.01:
                    alerts.append({
                        "window_start": latest["window_start"],
                        "window_end": latest["window_end"],
                        "alert_type": "low_conversion",
                        "alert_level": "WARNING",
                        "alert_message": f"访客支付转化率 < 1%: UV={uv}, pay_users={pay_users_val}",
                        "metric_name": "conversion_rate",
                        "metric_value": pay_users_val / uv,
                    })

                # 写入告警
                for alert in alerts:
                    insert_alert(ch["url"], ch["user"], ch["password"], alert)
                    print(f"  ALERT [{alert['alert_level']}]: {alert['alert_message']}")

                if not alerts:
                    print(f"  OK: window {latest['window_start']}, pay_amount={latest.get('pay_amount', 0)}, pay_users={latest.get('pay_users', 0)}, uv={latest.get('uv', 0)}")

        except Exception as e:
            print(f"  Error: {e}")

        if args.once:
            break
        time.sleep(30)


if __name__ == "__main__":
    main()
