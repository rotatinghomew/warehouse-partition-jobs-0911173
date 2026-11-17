import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .calendar import date_map, week_of
from .engine import ingest
from .plan import explain_recompute, weekly_lineage
from .publication import Scheduler, publish
from .runner import (advance_run, create_run, current_snapshot, read_current,
                     refresh_run, switch_current)

CONFIG_PATH = str(Path(__file__).resolve().parent.parent / 'calendar.json')


def run(root):
    """原有显式输入演示，保持不变。"""
    from .engine import run_day, run_week
    scheduler = Scheduler(root)
    try:
        version = ingest(scheduler.db, '东', '2026-11-01',
                         [{'region': '东', 'day': '2026-11-01', 'people': 12}])
        run_day(scheduler.db, '东', '2026-11-01', version)
        checksum = run_week(scheduler.db, '2026-10-26',
                            [('东', '2026-11-01', version)])
        return {'业务日映射': date_map('2026-11-01', 'Asia/Shanghai'),
                '发布': publish(scheduler.db, '周度摘录', '2026-10-26', checksum)}
    finally:
        scheduler.close()


def run_backfill(root):
    """端到端演示：周内分区到齐 -> 首次切换 -> 迟到更正 -> 防陈旧 -> 再切换。"""
    scheduler = Scheduler(root)
    clock = datetime(2026, 11, 16, 2, 0, tzinfo=timezone.utc)
    target_day = (datetime.fromisoformat('2026-11-16').date() - timedelta(days=7)).isoformat()
    week = week_of(target_day)
    start = datetime.fromisoformat(week).date()
    days = [(start + timedelta(days=i)).isoformat() for i in range(7)]
    regions = ['东', '西']
    report = {'配置': CONFIG_PATH, '业务周': week, '时钟': clock.isoformat()}
    try:
        for day in days:
            for region in regions:
                ingest(scheduler.db, region, day,
                       [{'region': region, 'day': day, 'people': 10}])
        first = create_run(scheduler.db, 'backfill',
                           [('东', target_day)], CONFIG_PATH, clock, run_id='bf-初次')
        report['初次计划'] = {'状态': first['status'], '固定输入数': len(first['pins']),
                          '步骤数': len(first['steps']), '未到齐': first['missing']}
        advance_run(scheduler.db, first['run_id'])
        switch_current(scheduler.db, first['run_id'], clock)
        before = current_snapshot(scheduler.db, week)
        report['初次切换后周汇总人数'] = before['weekly']['people']

        # 旧执行者在更正到达前先固定 v1 并完成计算。
        stale = create_run(scheduler.db, 'backfill',
                           [('东', target_day)], CONFIG_PATH, clock, run_id='bf-旧执行者')
        advance_run(scheduler.db, stale['run_id'])

        # 迟到更正：东区目标日人数从 10 改为 25，产生源版本 v2。
        ingest(scheduler.db, '东', target_day,
               [{'region': '东', 'day': target_day, 'people': 25}])
        report['补报解释'] = explain_recompute(scheduler.db, '东', target_day, CONFIG_PATH)

        # 旧执行者中断恢复后尝试切换固定 v1 的结果：必须被防陈旧机制拒绝。
        try:
            switch_current(scheduler.db, stale['run_id'], clock)
        except Exception as exc:
            report['旧执行者切换结果'] = str(exc.args[0])

        # 值班刷新运行、重新固定 v2，只重做受影响步骤后再切换。
        renewed = refresh_run(scheduler.db, stale['run_id'], clock)
        advance_run(scheduler.db, renewed['run_id'])
        switch_current(scheduler.db, renewed['run_id'], clock)
        after = current_snapshot(scheduler.db, week)
        report['再次切换后周汇总人数'] = after['weekly']['people']
        report['下游当前快照'] = {'fence': after['fence'], 'run_id': after['run_id'],
                              '日条目数': len(after['daily'])}
        checksum = _weekly_checksum(scheduler, week)
        report['封存发布'] = publish(scheduler.db, '周度封存', week, checksum)['people']
        report['发布血缘'] = weekly_lineage(scheduler.db, week, checksum)
        report['当前条目数'] = len(read_current(scheduler.db))
        return report
    finally:
        scheduler.close()


def _weekly_checksum(scheduler, week):
    row = scheduler.db.execute(
        'SELECT checksum FROM weekly WHERE week=? ORDER BY rowid DESC LIMIT 1',
        (week,)).fetchone()
    return row['checksum']
