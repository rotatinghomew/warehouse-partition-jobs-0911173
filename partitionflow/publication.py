import json

from .storage import atomic, connect

DDL="""
CREATE TABLE IF NOT EXISTS source_partitions(region TEXT,day TEXT,version INTEGER,checksum TEXT,payload TEXT,PRIMARY KEY(region,day,version));
CREATE TABLE IF NOT EXISTS daily(region TEXT,day TEXT,version INTEGER,payload TEXT,source_checksum TEXT,PRIMARY KEY(region,day,version));
CREATE TABLE IF NOT EXISTS weekly(week TEXT,checksum TEXT,inputs TEXT,people INTEGER,PRIMARY KEY(week,checksum));
CREATE TABLE IF NOT EXISTS releases(name TEXT PRIMARY KEY,payload TEXT);
"""


class Scheduler:
    def __init__(self,root):
        self.db=connect(root,DDL)

    def close(self):
        self.db.close()


def publish(database,name,week,checksum):
    with atomic(database):
        weekly=database.execute('SELECT * FROM weekly WHERE week=? AND checksum=?',(week,checksum)).fetchone()
        if not weekly:
            raise KeyError((week,checksum))
        daily=[]
        for region,day,version in json.loads(weekly['inputs']):
            row=database.execute('SELECT payload FROM daily WHERE region=? AND day=? AND version=?',(region,day,version)).fetchone()
            daily.append(json.loads(row[0]))
        payload={'week':week,'people':weekly['people'],'daily':daily,'inputs':json.loads(weekly['inputs'])}
        encoded=json.dumps(payload,ensure_ascii=False,sort_keys=True)
        old=database.execute('SELECT payload FROM releases WHERE name=?',(name,)).fetchone()
        if old and old[0]!=encoded:
            raise ValueError("发布名称不可重用")
        database.execute('INSERT OR IGNORE INTO releases VALUES(?,?)',(name,encoded))
    return payload


def read(database,name):
    row=database.execute('SELECT payload FROM releases WHERE name=?',(name,)).fetchone()
    if not row:
        raise KeyError(name)
    return json.loads(row[0])
