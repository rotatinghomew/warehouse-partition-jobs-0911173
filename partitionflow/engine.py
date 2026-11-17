import json

from .calendar import week_of
from .storage import atomic, fingerprint, records


def ingest(database, region, day, rows):
    from datetime import date
    date.fromisoformat(day)
    checksum=fingerprint(rows)
    for row in rows:
        if row['day'] != day or row['region'] != region or not isinstance(row['people'],int) or row['people']<0:
            raise ValueError("分区记录不符合地区日期范围")
    with atomic(database):
        version=database.execute('SELECT COALESCE(MAX(version),0)+1 FROM source_partitions WHERE region=? AND day=?',(region,day)).fetchone()[0]
        prior=database.execute('SELECT version FROM source_partitions WHERE region=? AND day=? AND checksum=?',(region,day,checksum)).fetchone()
        if prior:
            return prior[0]
        database.execute('INSERT INTO source_partitions VALUES(?,?,?,?,?)',(region,day,version,checksum,json.dumps(rows)))
    return version


def run_day(database,region,day,version):
    with atomic(database):
        source=database.execute('SELECT * FROM source_partitions WHERE region=? AND day=? AND version=?',(region,day,version)).fetchone()
        if not source:
            raise KeyError((region,day,version))
        rows=json.loads(source['payload'])
        result={'region':region,'day':day,'week':week_of(day),'people':sum(row['people'] for row in rows),'events':len(rows)}
        database.execute('INSERT OR IGNORE INTO daily VALUES(?,?,?,?,?)',(region,day,version,json.dumps(result),source['checksum']))
    return result


def run_week(database,week,inputs):
    with atomic(database):
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
        database.execute('INSERT OR IGNORE INTO weekly VALUES(?,?,?,?)',(week,checksum,json.dumps(normalized),total))
    return checksum
