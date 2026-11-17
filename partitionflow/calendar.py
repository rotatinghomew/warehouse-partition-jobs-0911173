from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def business_day(timestamp, timezone):
    instant=datetime.fromisoformat(timestamp)
    if instant.tzinfo is None:
        raise ValueError("事件时间缺少时区")
    return instant.astimezone(ZoneInfo(timezone)).date().isoformat()


def week_of(day):
    value=date.fromisoformat(day)
    return (value-timedelta(days=value.weekday())).isoformat()


def date_map(day, timezone):
    local=datetime.combine(date.fromisoformat(day),datetime.min.time(),ZoneInfo(timezone))
    end=local+timedelta(days=1)
    return {'local_day':day,'timezone':timezone,'utc_start':local.astimezone(ZoneInfo('UTC')).isoformat(),
            'utc_end':end.astimezone(ZoneInfo('UTC')).isoformat(),'week':week_of(day)}


def configured_map(day, region, config):
    import json
    from pathlib import Path
    settings=json.loads(Path(config).read_text(encoding='utf-8'))
    if region not in settings['regions']:
        raise ValueError("地区缺少业务时区")
    return date_map(day,settings['regions'][region])
