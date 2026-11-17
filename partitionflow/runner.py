"""补算运行器：固定输入版本、可恢复执行、防陈旧发布与原子一致切换。

设计要点：
- run 把计划时刻的源版本固定在 pins 中；切换前再次核对，任何分区出现
  更新的源版本都会拒绝发布（StaleRunError），旧执行者恢复后无法把旧
  输入算出的结果发布成最新。
- 步骤内容寻址、幂等；advance_run 只推进未成功的步骤，中断后再次调用
  即从断点继续，已成功的日/周结果不会重算，更不需要清库。
- current_entries 是消费者读取“当前一套完整结果”的唯一入口；切换在
  单事务内完成，下游在切换前后都读到自洽的一整套明细、日汇总、周汇总。
- backfill 计划在创建与切换两个时刻都核对业务日边界，历史补算永远不
  能覆盖当日批次的条目。
"""
import json
import uuid
from datetime import datetime, timezone

from .engine import run_day_tx, run_week_tx
from .plan import build_plan, current_business_day, expected_partitions, load_config
from .storage import atomic, fingerprint


class RunError(RuntimeError):
    """补算运行的可预期错误。"""


class StaleRunError(RunError):
    """运行固定的输入版本已被更新的源分区超越。"""


class StepFailedError(RunError):
    """步骤计算失败；已成功步骤保留，可再次 advance_run 重试。"""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _load_row(database, run_id):
    row = database.execute('SELECT * FROM runs WHERE run_id=?', (run_id,)).fetchone()
    if row is None:
        raise KeyError(run_id)
    return row


def create_run(database, kind, targets, config_path, now, run_id=None):
    plan = build_plan(database, kind, targets, config_path, now)
    run_id = run_id or f"{kind}-{uuid.uuid4().hex[:12]}"
    stamp = _now()
    with atomic(database):
        fence = database.execute('SELECT COALESCE(MAX(fence),0)+1 FROM runs').fetchone()[0]
        status = 'blocked' if plan['missing'] else 'ready'
        database.execute('INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?)',
                         (run_id, kind, json.dumps(plan, ensure_ascii=False, sort_keys=True),
                          status, fence, str(config_path), now.isoformat(), stamp, stamp))
        for step in plan['steps']:
            database.execute('INSERT INTO run_steps VALUES(?,?,?,?,?,?,?,?,?,?)',
                             (run_id, step['step_id'], step['scope'],
                              json.dumps(step['target'], ensure_ascii=False),
                              json.dumps(step['pinned'], ensure_ascii=False),
                              step['status'], step.get('result_ref'), 0, None, stamp))
    return describe_run(database, run_id)


def describe_run(database, run_id):
    row = _load_row(database, run_id)
    spec = json.loads(row['spec'])
    steps = [dict(item) for item in database.execute(
        'SELECT step_id,scope,target,pinned,status,result_ref,attempts,error '
        'FROM run_steps WHERE run_id=? ORDER BY step_id', (run_id,)).fetchall()]
    for step in steps:
        step['target'] = json.loads(step['target'])
        step['pinned'] = json.loads(step['pinned'])
    return {'run_id': run_id, 'kind': row['kind'], 'status': row['status'],
            'fence': row['fence'], 'missing': spec['missing'], 'pins': spec['pins'],
            'entries': spec['entries'], 'steps': steps,
            'created_at': row['created_at'], 'updated_at': row['updated_at']}


def list_runs(database):
    return [describe_run(database, row['run_id'])
            for row in database.execute('SELECT run_id FROM runs ORDER BY fence').fetchall()]


def refresh_run(database, run_id, now):
    """迟到分区到齐后按同一目标重新固定版本并重建步骤。

    已成功的内容寻址步骤保留；新出现的源版本会改变 pins，并抬高 fence，
    使任何仍持有旧计划的执行者在切换时被 StaleRunError 拒绝。
    """
    stamp = _now()
    with atomic(database):
        row = _load_row(database, run_id)
        if row['status'] in ('switched',):
            raise RunError(f"运行 {run_id} 已切换，不能刷新")
        old_spec = json.loads(row['spec'])
        plan = build_plan(database, row['kind'], old_spec['targets'],
                          row['config_path'], now)
        new_fence = database.execute('SELECT COALESCE(MAX(fence),0)+1 FROM runs').fetchone()[0]
        status = 'blocked' if plan['missing'] else 'ready'
        database.execute(
            'UPDATE runs SET spec=?,status=?,fence=?,clock=?,updated_at=? WHERE run_id=?',
            (json.dumps(plan, ensure_ascii=False, sort_keys=True), status, new_fence,
             now.isoformat(), stamp, run_id))
        database.execute('DELETE FROM run_steps WHERE run_id=?', (run_id,))
        for step in plan['steps']:
            database.execute('INSERT INTO run_steps VALUES(?,?,?,?,?,?,?,?,?,?)',
                             (run_id, step['step_id'], step['scope'],
                              json.dumps(step['target'], ensure_ascii=False),
                              json.dumps(step['pinned'], ensure_ascii=False),
                              step['status'], step.get('result_ref'), 0, None, stamp))
    return describe_run(database, run_id)


def _execute_step(database, step):
    if step['scope'] == 'daily':
        region, day = step['target']
        _, _, version = step['pinned']
        result = run_day_tx(database, region, day, version)
        return json.dumps({'region': region, 'day': day, 'version': version},
                          ensure_ascii=False, sort_keys=True), result
    if step['scope'] == 'weekly':
        week = step['target']
        checksum = run_week_tx(database, week, [tuple(item) for item in step['pinned']])
        return checksum, {'week': week, 'checksum': checksum}
    raise RunError(f"未知步骤类型 {step['scope']}")


def advance_run(database, run_id):
    """执行所有未成功的步骤；已成功步骤跳过，失败步骤记录错误并抛出。"""
    failures = []
    with atomic(database):
        row = _load_row(database, run_id)
        if row['status'] in ('switched', 'superseded'):
            raise RunError(f"运行 {run_id} 已处于 {row['status']}，不能再执行")
        step_rows = database.execute(
            'SELECT * FROM run_steps WHERE run_id=? ORDER BY CASE scope '
            "WHEN 'daily' THEN 0 ELSE 1 END, step_id", (run_id,)).fetchall()
        for step_row in step_rows:
            if step_row['status'] in ('done', 'blocked'):
                continue
            step = {'scope': step_row['scope'], 'target': json.loads(step_row['target']),
                    'pinned': json.loads(step_row['pinned'])}
            attempts = step_row['attempts'] + 1
            try:
                result_ref, _ = _execute_step(database, step)
                database.execute(
                    'UPDATE run_steps SET status=?,result_ref=?,attempts=?,error=?,updated_at=? '
                    'WHERE run_id=? AND step_id=?',
                    ('done', result_ref, attempts, None, _now(), run_id, step_row['step_id']))
            except Exception as exc:  # 失败被保留，调用者修复后可重试
                database.execute(
                    'UPDATE run_steps SET status=?,attempts=?,error=?,updated_at=? '
                    'WHERE run_id=? AND step_id=?',
                    ('failed', attempts, f"{type(exc).__name__}: {exc}", _now(),
                     run_id, step_row['step_id']))
                failures.append({'step_id': step_row['step_id'], 'error': str(exc)})
        pending = database.execute(
            "SELECT COUNT(*) FROM run_steps WHERE run_id=? AND status IN ('pending','failed')",
            (run_id,)).fetchone()[0]
        blocked = database.execute(
            "SELECT COUNT(*) FROM run_steps WHERE run_id=? AND status='blocked'",
            (run_id,)).fetchone()[0]
        new_status = 'failed' if failures else ('blocked' if blocked else
                                                ('ready' if pending == 0 else 'running'))
        database.execute('UPDATE runs SET status=?,updated_at=? WHERE run_id=?',
                         (new_status, _now(), run_id))
    if failures:
        raise StepFailedError({'run_id': run_id, 'failures': failures})
    return describe_run(database, run_id)


def _assert_pins_fresh(database, spec):
    """固定版本与当前最新源版本逐分区核对，返回超越本运行的分区。"""
    stale = []
    for region, day, version in spec['pins']:
        row = database.execute(
            'SELECT MAX(version) AS v FROM source_partitions WHERE region=? AND day=?',
            (region, day)).fetchone()
        if row['v'] is not None and row['v'] > version:
            stale.append({'region': region, 'day': day, 'pinned_version': version,
                          'latest_version': row['v']})
    return stale


def _assert_guard(database, row, spec, now):
    """切换时再次执行业务日边界保护，防止时钟推进后补算覆盖当日批次。"""
    if spec['kind'] != 'backfill':
        return
    settings = load_config(row['config_path'])
    for region, day in spec['targets']:
        if not day < current_business_day(now, settings['regions'][region]):
            raise RunError("目标分区已进入当日批次范围，历史补算不得切换覆盖；"
                           "请改走当日批次处理")


def switch_current(database, run_id, now):
    """把该运行的结果原子切换为消费者当前读取的一套完整结果。"""
    with atomic(database):
        row = _load_row(database, run_id)
        spec = json.loads(row['spec'])
        if row['status'] == 'switched':
            return describe_run(database, run_id)
        if row['status'] == 'superseded':
            raise StaleRunError(f"运行 {run_id} 已被更新的补算取代")
        stale = _assert_pins_fresh(database, spec)
        if stale:
            database.execute('UPDATE runs SET status=?,updated_at=? WHERE run_id=?',
                             ('superseded', _now(), run_id))
            raise StaleRunError({'run_id': run_id, 'stale_partitions': stale})
        _assert_guard(database, row, spec, now)
        blocked = database.execute(
            "SELECT COUNT(*) FROM run_steps WHERE run_id=? AND status!='done'",
            (run_id,)).fetchone()[0]
        if blocked:
            raise RunError("仍有依赖未到齐或未成功，不能切换；下游继续读取上一套完整结果")

        for entry in spec['entries']:
            scope, key = entry['scope'], entry['key']
            if scope == 'daily':
                region, day = key.split('|', 1)
                step = database.execute(
                    "SELECT result_ref,pinned FROM run_steps WHERE run_id=? AND step_id=?",
                    (run_id, f"daily:{region}:{day}")).fetchone()
                version = json.loads(step['pinned'])[2]
                daily = database.execute(
                    'SELECT payload,source_checksum FROM daily WHERE region=? AND day=? '
                    'AND version=?', (region, day, version)).fetchone()
                payload = daily['payload']
                pins = [json.loads(step['pinned'])]
            else:
                step = database.execute(
                    "SELECT result_ref,pinned FROM run_steps WHERE run_id=? AND step_id=?",
                    (run_id, f"weekly:{key}")).fetchone()
                weekly = database.execute(
                    'SELECT inputs,people FROM weekly WHERE week=? AND checksum=?',
                    (key, step['result_ref'])).fetchone()
                payload = json.dumps(
                    {'week': key, 'people': weekly['people'],
                     'inputs': json.loads(weekly['inputs'])},
                    ensure_ascii=False, sort_keys=True)
                pins = json.loads(step['pinned'])
            # CAS：只在 fence 不小于现状时写入，两个执行者同时切换时后到者不覆盖。
            changed = database.execute(
                'INSERT INTO current_entries(scope,key,payload,pins,fence,run_id,updated_at) '
                'VALUES(?,?,?,?,?,?,?) ON CONFLICT(scope,key) DO UPDATE SET '
                'payload=excluded.payload,pins=excluded.pins,fence=excluded.fence,'
                'run_id=excluded.run_id,updated_at=excluded.updated_at '
                'WHERE excluded.fence>=current_entries.fence',
                (scope, key, payload, json.dumps(pins, ensure_ascii=False, sort_keys=True),
                 row['fence'], run_id, _now())).rowcount
            if changed == 0:
                existing = database.execute(
                    'SELECT run_id FROM current_entries WHERE scope=? AND key=?',
                    (scope, key)).fetchone()
                raise StaleRunError({'run_id': run_id, 'entry': entry,
                                     'current_owner': existing['run_id'] if existing else None})
        database.execute('UPDATE runs SET status=?,updated_at=? WHERE run_id=?',
                         ('switched', _now(), run_id))
    return describe_run(database, run_id)


def read_current(database, scope=None):
    """读取当前一套完整结果；单事务快照，保证各条目来自同一视角。"""
    with atomic(database):
        sql = 'SELECT scope,key,payload,pins,fence,run_id FROM current_entries'
        parameters = ()
        if scope is not None:
            sql += ' WHERE scope=?'
            parameters = (scope,)
        sql += ' ORDER BY scope,key'
        entries = [{'scope': row['scope'], 'key': row['key'],
                    'payload': json.loads(row['payload']),
                    'pins': json.loads(row['pins']), 'fence': row['fence'],
                    'run_id': row['run_id']}
                   for row in database.execute(sql, parameters).fetchall()]
    if not entries:
        raise KeyError(scope or 'current')
    return entries


def current_snapshot(database, week):
    """下游视角：一次读取某周自洽的明细、日汇总与周汇总；未整体就绪则报错。"""
    with atomic(database):
        weekly_row = database.execute(
            'SELECT * FROM current_entries WHERE scope=? AND key=?', ('weekly', week)).fetchone()
        if weekly_row is None:
            raise KeyError(('weekly', week))
        run_row = _load_row(database, weekly_row['run_id'])
        expected = {f"{region}|{day}"
                    for region, day in expected_partitions(run_row['config_path'], week)}
        daily_rows = {row['key']: row for row in database.execute(
            "SELECT * FROM current_entries WHERE scope='daily'").fetchall()}
        missing = sorted(expected - set(daily_rows))
        if missing:
            raise RunError({'week': week, 'missing_current_entries': missing})
        if any(row['fence'] != weekly_row['fence'] or row['run_id'] != weekly_row['run_id']
               for key, row in daily_rows.items() if key in expected):
            raise RunError("当前条目跨越不同切换，未形成一致快照；请等待本次切换完成")
        return {'week': week, 'fence': weekly_row['fence'], 'run_id': weekly_row['run_id'],
                'weekly': json.loads(weekly_row['payload']),
                'daily': [json.loads(daily_rows[key]['payload']) for key in sorted(expected)]}


def entry_fingerprint(entry):
    return fingerprint({'scope': entry['scope'], 'key': entry['key'],
                        'payload': entry['payload']})
