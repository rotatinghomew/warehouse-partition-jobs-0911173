"""按真实输入依赖制定补算计划，并解释结果与发布的血缘。

本模块只做只读分析与计划生成，不发布任何结果。计划把每个依赖分区
固定到计划时刻的最新源版本（pin），后续执行者必须凭该固定版本计算，
从而可以回答“某个输出用了哪些源分区版本、哪些分区还没到齐”。
"""
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .calendar import business_day, week_of


def load_config(config_path):
    settings = json.loads(Path(config_path).read_text(encoding='utf-8'))
    if not settings.get('regions'):
        raise ValueError("配置缺少地区业务时区")
    return settings


def week_days(week):
    start = date.fromisoformat(week)
    if start.weekday() != 0:
        raise ValueError("周键必须是周一日期")
    return [(start + timedelta(days=offset)).isoformat() for offset in range(7)]


def current_business_day(now, tz_name):
    """now 必须是带时区的瞬时；返回该地区在该瞬时的本地业务日。"""
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("当前瞬时必须带时区")
    return business_day(now.astimezone(timezone.utc).isoformat(), tz_name)


def expected_partitions(config_path, week):
    """一个业务周按配置应到齐的全部 (地区, 业务日) 分区。"""
    settings = load_config(config_path)
    return [(region, day) for day in week_days(week) for region in settings['regions']]


def latest_source(database, region, day):
    row = database.execute(
        'SELECT version,checksum FROM source_partitions WHERE region=? AND day=? '
        'ORDER BY version DESC LIMIT 1', (region, day)).fetchone()
    if row is None:
        return None
    return {'region': region, 'day': day, 'version': row['version'], 'checksum': row['checksum']}


def missing_partitions(database, config_path, week):
    """该业务周尚未到齐（没有任何源版本）的分区。"""
    return [{'region': region, 'day': day}
            for region, day in expected_partitions(config_path, week)
            if latest_source(database, region, day) is None]


def week_ready(database, config_path, week):
    missing = missing_partitions(database, config_path, week)
    return {'week': week, 'ready': not missing, 'missing': missing}


def _pin(database, region, day):
    source = latest_source(database, region, day)
    if source is None:
        return None
    return [region, day, source['version']]


def build_plan(database, kind, targets, config_path, now):
    """为补报目标（(region, day) 列表）生成固定版本的补算计划。

    kind='backfill' 只允许历史业务日；kind='daily' 只允许各地区当日。
    计划自动把受影响业务周内尚未计算的日结果补成依赖步骤，并为每个受
    影响的周生成周步骤；缺源的分区列入 missing，对应步骤保持 blocked。
    """
    if kind not in ('backfill', 'daily'):
        raise ValueError("运行类型必须是 backfill 或 daily")
    settings = load_config(config_path)
    norm_targets = sorted({(require_region_day(item)[0], require_region_day(item)[1])
                           for item in targets})
    today = {region: current_business_day(now, tz_name)
             for region, tz_name in settings['regions'].items()}
    for region, day in norm_targets:
        if region not in settings['regions']:
            raise ValueError(f"地区 {region} 缺少业务时区")
        if kind == 'backfill' and not day < today[region]:
            raise ValueError("历史补算只能针对各地区当前业务日之前的分区")
        if kind == 'daily' and day != today[region]:
            raise ValueError("当日批次只能处理该地区的当前业务日，历史分区请走 backfill")

    affected_weeks = sorted({week_of(day) for _, day in norm_targets})
    pinned, missing = [], []
    steps = []

    if kind == 'daily':
        # 当日批次只负责各地区当前业务日的日条目；周汇总等周内分区到齐后
        # 由 backfill 运行统一切换，避免未来日阻塞当日发布。
        day_scope = set(norm_targets)
    else:
        # 目标分区与受影响周内其它应到齐分区共同构成日步骤集合。
        day_scope = set(norm_targets)
        for week in affected_weeks:
            day_scope.update(expected_partitions(config_path, week))

    for region, day in sorted(day_scope):
        pin = _pin(database, region, day)
        if pin is None:
            missing.append({'region': region, 'day': day})
            steps.append({'step_id': f"daily:{region}:{day}", 'scope': 'daily',
                          'target': [region, day], 'pinned': None, 'status': 'blocked'})
            continue
        pinned.append(pin)
        existing = database.execute(
            'SELECT 1 FROM daily WHERE region=? AND day=? AND version=?',
            (region, day, pin[2])).fetchone()
        status = 'done' if existing else 'pending'
        steps.append({'step_id': f"daily:{region}:{day}", 'scope': 'daily',
                      'target': [region, day], 'pinned': pin, 'status': status})

    weekly_steps = []
    if kind == 'backfill':
        for week in affected_weeks:
            inputs, week_missing = [], []
            for region, day in expected_partitions(config_path, week):
                pin = _pin(database, region, day)
                if pin is None:
                    week_missing.append({'region': region, 'day': day})
                    continue
                inputs.append(pin)
            for item in week_missing:
                if item not in missing:
                    missing.append(item)
            if week_missing:
                weekly_steps.append({'step_id': f"weekly:{week}", 'scope': 'weekly',
                                     'target': week, 'pinned': None, 'status': 'blocked'})
                continue
            checksum = existing_weekly_checksum(database, inputs)
            weekly_steps.append({'step_id': f"weekly:{week}", 'scope': 'weekly', 'target': week,
                                 'pinned': sorted(inputs),
                                 'status': 'done' if checksum else 'pending',
                                 'result_ref': checksum})

    steps.extend(weekly_steps)
    entries = [{'scope': 'daily', 'key': f"{region}|{day}"} for region, day in sorted(day_scope)]
    if kind == 'backfill':
        entries += [{'scope': 'weekly', 'key': week} for week in affected_weeks]
    return {'kind': kind, 'targets': [list(item) for item in norm_targets],
            'weeks': affected_weeks, 'pins': sorted(pinned), 'missing': missing,
            'steps': steps, 'entries': entries,
            'config_path': str(config_path), 'clock': now.isoformat()}


def existing_weekly_checksum(database, inputs):
    wanted = json.dumps(sorted(inputs), ensure_ascii=False, sort_keys=True)
    for candidate in database.execute('SELECT checksum,inputs FROM weekly').fetchall():
        if json.dumps(sorted(json.loads(candidate['inputs'])),
                      ensure_ascii=False, sort_keys=True) == wanted:
            return candidate['checksum']
    return None


def require_region_day(item):
    if len(item) != 2:
        raise ValueError("分区目标必须是 (地区, 业务日)")
    region, day = item
    if not isinstance(region, str) or not region.strip():
        raise ValueError("地区需要非空字符串")
    date.fromisoformat(day)
    return region, day


def explain_recompute(database, region, day, config_path):
    """解释一次针对某源分区的补报为什么需要重算这些结果。"""
    source = latest_source(database, region, day)
    if source is None:
        raise KeyError((region, day))
    week = week_of(day)
    history = database.execute(
        'SELECT version,checksum FROM source_partitions WHERE region=? AND day=? '
        'ORDER BY version', (region, day)).fetchall()
    deliveries = database.execute(
        'SELECT COUNT(*) AS n FROM source_deliveries WHERE region=? AND day=? AND checksum=?',
        (region, day, source['checksum'])).fetchone()['n']
    earlier = [item for item in history if item['version'] != source['version']]
    previous_version = earlier[-1]['version'] if earlier else None
    if deliveries > 1:
        reason = (f"第 {deliveries} 次交付与 v{source['version']} 摘要一致，"
                  "复用既有结果，无需重算")
        changed = False
    elif earlier:
        reason = (f"源分区内容发生更正：v{previous_version} -> "
                  f"v{source['version']}，内容摘要不同，按完整更正重算，不逐行叠加")
        changed = True
    else:
        reason = '首次到齐的分区，需要建立基线结果'
        changed = True

    expected = expected_partitions(config_path, week)
    arrival = [{'region': r, 'day': d, **(latest_source(database, r, d)
                                          or {'version': None, 'checksum': None,
                                              'arrived': False})}
               for r, d in expected]
    for item in arrival:
        item.setdefault('arrived', True)
    return {'source': {'region': region, 'day': day, 'version': source['version'],
                       'checksum': source['checksum']},
            'previous_version': previous_version,
            'content_changed': changed, 'reason': reason,
            'daily_recalc': changed,
            'week': week,
            'weekly_recalc': changed,
            'weekly_reason': '全国周汇总绑定固定输入版本集合，任一来源版本变化都会得到新的周结果',
            'week_arrival': arrival,
            'week_missing': [{'region': item['region'], 'day': item['day']}
                             for item in arrival if not item['arrived']]}


def daily_lineage(database, region, day, version=None):
    source = latest_source(database, region, day) if version is None else {'version': version}
    chosen = version if version is not None else (source['version'] if source else None)
    daily = database.execute(
        'SELECT d.payload,d.source_checksum,s.checksum AS source_check '
        'FROM daily d LEFT JOIN source_partitions s '
        'ON s.region=d.region AND s.day=d.day AND s.version=d.version '
        'WHERE d.region=? AND d.day=? AND d.version=?',
        (region, day, chosen)).fetchone()
    if daily is None:
        raise KeyError(('daily', region, day, chosen))
    latest = latest_source(database, region, day)
    return {'output': {'scope': 'daily', 'region': region, 'day': day, 'version': chosen},
            'inputs': [{'region': region, 'day': day, 'version': chosen,
                        'checksum': daily['source_check']}],
            'latest_source_version': latest['version'] if latest else None,
            'up_to_date': bool(latest and latest['version'] == chosen)}


def weekly_lineage(database, week, checksum=None):
    if checksum is None:
        row = database.execute(
            'SELECT checksum,inputs FROM weekly WHERE week=? ORDER BY rowid DESC LIMIT 1',
            (week,)).fetchone()
        if row is None:
            raise KeyError(('weekly', week))
        checksum = row['checksum']
    weekly = database.execute('SELECT * FROM weekly WHERE week=? AND checksum=?',
                              (week, checksum)).fetchone()
    if weekly is None:
        raise KeyError(('weekly', week, checksum))
    inputs = []
    for region, day, version in json.loads(weekly['inputs']):
        source = database.execute(
            'SELECT checksum FROM source_partitions WHERE region=? AND day=? AND version=?',
            (region, day, version)).fetchone()
        latest = latest_source(database, region, day)
        inputs.append({'region': region, 'day': day, 'version': version,
                       'checksum': source['checksum'] if source else None,
                       'latest_version': latest['version'] if latest else None,
                       'up_to_date': bool(latest and latest['version'] == version)})
    return {'output': {'scope': 'weekly', 'week': week, 'checksum': checksum},
            'inputs': inputs,
            'up_to_date': all(item['up_to_date'] for item in inputs)}


def release_lineage(database, name):
    """封存发布的血缘：它绑定的周结果与每个源分区版本（含是否已被后来版本超越）。"""
    from .publication import read
    payload = read(database, name)
    week = payload['week']
    checksum = None
    for candidate in database.execute('SELECT checksum,inputs FROM weekly WHERE week=?',
                                      (week,)).fetchall():
        if candidate['inputs'] == json.dumps(payload['inputs'], ensure_ascii=False,
                                              sort_keys=True):
            checksum = candidate['checksum']
            break
    return {'release': name, 'frozen': True,
            'weekly_lineage': weekly_lineage(database, week, checksum)}
