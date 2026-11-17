import json
from datetime import datetime, timezone

from .calendar import week_of
from .storage import atomic, fingerprint


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def ingest(database, region, day, rows, seen_at=None):
    """登记源分区版本。seen_at 记录本次交付瞬时；相同内容重传追加交付记录。"""
    from datetime import date
    date.fromisoformat(day)
    checksum=fingerprint(rows)
    for row in rows:
        if row['day'] != day or row['region'] != region or not isinstance(row['people'],int) or row['people']<0:
            raise ValueError("分区记录不符合地区日期范围")
    seen_at = seen_at or _utc_now()
    with atomic(database):
        database.execute('INSERT OR IGNORE INTO source_deliveries VALUES(?,?,?,?)',
                         (region,day,checksum,seen_at))
        prior=database.execute('SELECT version FROM source_partitions WHERE region=? AND day=? AND checksum=?',(region,day,checksum)).fetchone()
        if prior:
            return prior[0]
        version=database.execute('SELECT COALESCE(MAX(version),0)+1 FROM source_partitions WHERE region=? AND day=?',(region,day)).fetchone()[0]
        database.execute('INSERT INTO source_partitions VALUES(?,?,?,?,?)',(region,day,version,checksum,json.dumps(rows)))
    return version


def run_day_tx(database,region,day,version):
    """事务内核心：调用方必须已经持有该连接上的事务。"""
    source=database.execute('SELECT * FROM source_partitions WHERE region=? AND day=? AND version=?',(region,day,version)).fetchone()
    if not source:
        raise KeyError((region,day,version))
    rows=json.loads(source['payload'])
    result={'region':region,'day':day,'week':week_of(day),'people':sum(row['people'] for row in rows),'events':len(rows)}
    database.execute('INSERT OR IGNORE INTO daily VALUES(?,?,?,?,?)',(region,day,version,json.dumps(result,ensure_ascii=False,sort_keys=True),source['checksum']))
    return result


def run_day(database,region,day,version):
    with atomic(database):
        return run_day_tx(database,region,day,version)


def run_week_tx(database,week,inputs):
    """事务内核心：调用方必须已经持有该连接上的事务。"""
    total=0
    normalized=sorted(inputs)
    if len({(region,day) for region,day,version in normalized}) != len(normalized):
        raise ValueError("同一输入分区出现多次")
    for region,day,version in normalized:
        if week_of(day)!=week:
            raise ValueError("输入不属于目标周")
        row=database.execute('SELECT payload FROM daily WHERE region=? AND day=? AND version=?',(region,day,version)).fetchone()
        if not row:
            raise ValueError("日作业结果未就绪")
        total+=json.loads(row[0])['people']
    checksum=fingerprint(normalized)
    database.execute('INSERT OR IGNORE INTO weekly VALUES(?,?,?,?)',(week,checksum,json.dumps(normalized,ensure_ascii=False,sort_keys=True),total))
    return checksum


def run_week(database,week,inputs):
    with atomic(database):
        return run_week_tx(database,week,inputs)
