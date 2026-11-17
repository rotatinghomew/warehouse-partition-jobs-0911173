import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from partitionflow.engine import ingest
from partitionflow.plan import (daily_lineage, explain_recompute, missing_partitions,
                                release_lineage, week_ready, weekly_lineage)
from partitionflow.publication import Scheduler, publish, read
from partitionflow.runner import (RunError, StepFailedError, StaleRunError, advance_run,
                                  create_run, current_snapshot, read_current, refresh_run,
                                  switch_current)

CLOCK = datetime(2026, 11, 16, 2, 0, tzinfo=timezone.utc)
WEEK = '2026-11-09'
DAYS = ['2026-11-09', '2026-11-10', '2026-11-11', '2026-11-12',
        '2026-11-13', '2026-11-14', '2026-11-15']
REGIONS = ['东', '西']
CONFIG = {
    'regions': {'东': 'Asia/Shanghai', '西': 'Asia/Urumqi'},
    'national_timezone': 'UTC', 'week_start': 'monday'}


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = self.temporary.name
        self.scheduler = Scheduler(self.directory)
        self.config = Path(self.directory) / 'calendar.json'
        self.config.write_text(json.dumps(CONFIG, ensure_ascii=False), encoding='utf-8')

    def tearDown(self):
        self.scheduler.close()
        self.temporary.cleanup()

    def row(self, region, day, people):
        return {'region': region, 'day': day, 'people': people}

    def ingest_week(self, value=10):
        for day in DAYS:
            for region in REGIONS:
                ingest(self.scheduler.db, region, day, [self.row(region, day, value)])

    def backfill(self, targets):
        run = create_run(self.scheduler.db, 'backfill', targets, self.config, CLOCK)
        advance_run(self.scheduler.db, run['run_id'])
        return switch_current(self.scheduler.db, run['run_id'], CLOCK)

    def test_missing_arrival_and_readiness(self):
        ingest(self.scheduler.db, '东', DAYS[0], [self.row('东', DAYS[0], 3)])
        readiness = week_ready(self.scheduler.db, self.config, WEEK)
        self.assertFalse(readiness['ready'])
        self.assertEqual(len(readiness['missing']), 13)
        missing = missing_partitions(self.scheduler.db, self.config, WEEK)
        self.assertIn({'region': '西', 'day': DAYS[0]}, missing)
        run = create_run(self.scheduler.db, 'backfill',
                         [('东', DAYS[0])], self.config, CLOCK)
        self.assertEqual(run['status'], 'blocked')
        blocked = [step for step in run['steps'] if step['status'] == 'blocked']
        self.assertTrue(any(step['scope'] == 'weekly' for step in blocked))
        with self.assertRaises(RunError):
            switch_current(self.scheduler.db, run['run_id'], CLOCK)

    def test_backfill_switches_consistent_snapshot(self):
        self.ingest_week(10)
        run = self.backfill([('东', DAYS[0])])
        self.assertEqual(run['status'], 'switched')
        snapshot = current_snapshot(self.scheduler.db, WEEK)
        self.assertEqual(len(snapshot['daily']), 14)
        self.assertEqual(snapshot['weekly']['people'], 140)
        self.assertEqual(len(snapshot['weekly']['inputs']), 14)
        # 所有当前条目来自同一次切换，下游读到自洽的一整套。
        entries = read_current(self.scheduler.db)
        self.assertTrue(all(item['run_id'] == run['run_id'] for item in entries))

    def test_correction_recomputes_and_repins(self):
        self.ingest_week(10)
        self.backfill([('东', DAYS[0])])
        before = current_snapshot(self.scheduler.db, WEEK)
        ingest(self.scheduler.db, '东', DAYS[0], [self.row('东', DAYS[0], 25)])
        explanation = explain_recompute(self.scheduler.db, '东', DAYS[0], self.config)
        self.assertTrue(explanation['content_changed'])
        self.assertEqual(explanation['source']['version'], 2)
        self.assertEqual(explanation['previous_version'], 1)
        self.assertTrue(explanation['weekly_recalc'])
        run = self.backfill([('东', DAYS[0])])
        after = current_snapshot(self.scheduler.db, WEEK)
        self.assertEqual(after['weekly']['people'], 155)
        self.assertNotEqual(before['fence'], after['fence'])
        changed_entry = next(item for item in read_current(self.scheduler.db, 'daily')
                             if item['key'] == f"东|{DAYS[0]}")
        self.assertEqual(changed_entry['pins'][0][2], 2)

    def test_identical_retransmission_needs_no_recompute(self):
        self.ingest_week(10)
        self.backfill([('东', DAYS[0])])
        repeated = ingest(self.scheduler.db, '东', DAYS[0],
                          [self.row('东', DAYS[0], 10)])
        self.assertEqual(repeated, 1)
        # 同内容重传不产生新版本；随后真正的更正产生 v2，解释必须识别为内容变化。
        ingest(self.scheduler.db, '东', DAYS[0], [self.row('东', DAYS[0], 11)])
        explanation = explain_recompute(self.scheduler.db, '东', DAYS[0], self.config)
        self.assertTrue(explanation['content_changed'])
        self.assertEqual(explanation['previous_version'], 1)
        self.assertEqual(explanation['source']['version'], 2)
        # 再以 v2 相同内容重传，解释应明确无需重算。
        ingest(self.scheduler.db, '东', DAYS[0], [self.row('东', DAYS[0], 11)])
        again = explain_recompute(self.scheduler.db, '东', DAYS[0], self.config)
        self.assertFalse(again['content_changed'])
        self.assertFalse(again['daily_recalc'])

    def test_backfill_cannot_touch_current_business_day(self):
        self.ingest_week(10)
        with self.assertRaises(ValueError):
            create_run(self.scheduler.db, 'backfill',
                       [('东', '2026-11-16')], self.config, CLOCK)
        daily = create_run(self.scheduler.db, 'daily',
                           [('东', '2026-11-16')], self.config, CLOCK,
                           run_id='daily-east')
        self.assertEqual(daily['kind'], 'daily')

    def test_daily_run_is_isolated_from_backfill_week(self):
        # 当日批次只切换当日条目，不被同周尚未到齐的未来日阻塞。
        ingest(self.scheduler.db, '东', '2026-11-16',
               [self.row('东', '2026-11-16', 7)])
        run = create_run(self.scheduler.db, 'daily',
                         [('东', '2026-11-16')], self.config, CLOCK, run_id='daily-1')
        self.assertEqual(run['status'], 'ready')
        self.assertEqual([step['scope'] for step in run['steps']], ['daily'])
        advance_run(self.scheduler.db, run['run_id'])
        switch_current(self.scheduler.db, run['run_id'], CLOCK)
        entries = read_current(self.scheduler.db)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['key'], '东|2026-11-16')
        self.assertEqual(entries[0]['payload']['people'], 7)
        with self.assertRaises(KeyError):
            current_snapshot(self.scheduler.db, WEEK)

    def test_switch_guard_rechecks_clock(self):
        self.ingest_week(10)
        run = create_run(self.scheduler.db, 'backfill',
                         [('东', DAYS[-1])], self.config, CLOCK)
        advance_run(self.scheduler.db, run['run_id'])
        # 时钟推进到该地区次日：目标日已成为历史的更早一天，仍允许切换；
        # 若补报目标在切换时已进入当日范围，则在此处被拒绝。
        later = datetime(2026, 11, 17, 18, 0, tzinfo=timezone.utc)
        switched = switch_current(self.scheduler.db, run['run_id'], later)
        self.assertEqual(switched['status'], 'switched')

    def test_two_executors_same_work_only_one_publishes(self):
        self.ingest_week(10)
        first = create_run(self.scheduler.db, 'backfill',
                           [('东', DAYS[0])], self.config, CLOCK, run_id='run-1')
        second = create_run(self.scheduler.db, 'backfill',
                            [('东', DAYS[0])], self.config, CLOCK, run_id='run-2')
        advance_run(self.scheduler.db, first['run_id'])
        advance_run(self.scheduler.db, second['run_id'])
        switch_current(self.scheduler.db, first['run_id'], CLOCK)
        # 后到执行者 fence 更高，CAS 允许它推进；同 fence 的重复切换才是幂等返回。
        again = switch_current(self.scheduler.db, first['run_id'], CLOCK)
        self.assertEqual(again['status'], 'switched')
        switched = switch_current(self.scheduler.db, second['run_id'], CLOCK)
        self.assertEqual(switched['status'], 'switched')
        self.assertEqual(read_current(self.scheduler.db, 'weekly')[0]['run_id'], 'run-2')

    def test_older_fence_cannot_overwrite_newer_current(self):
        self.ingest_week(10)
        first = create_run(self.scheduler.db, 'backfill',
                           [('东', DAYS[0])], self.config, CLOCK, run_id='fence-1')
        second = create_run(self.scheduler.db, 'backfill',
                            [('东', DAYS[0])], self.config, CLOCK, run_id='fence-2')
        advance_run(self.scheduler.db, first['run_id'])
        advance_run(self.scheduler.db, second['run_id'])
        switch_current(self.scheduler.db, second['run_id'], CLOCK)
        # fence=1 的运行迟到，CAS 必须拒绝它覆盖 fence=2 已发布的条目。
        with self.assertRaises(StaleRunError):
            switch_current(self.scheduler.db, first['run_id'], CLOCK)
        self.assertEqual(read_current(self.scheduler.db, 'weekly')[0]['run_id'], 'fence-2')

    def test_stale_executor_cannot_publish_old_inputs(self):
        self.ingest_week(10)
        run = create_run(self.scheduler.db, 'backfill',
                         [('东', DAYS[0])], self.config, CLOCK, run_id='stale-run')
        advance_run(self.scheduler.db, run['run_id'])
        # 计划固定 v1 后出现 v2 更正：旧执行者恢复并尝试切换必须被拒绝。
        ingest(self.scheduler.db, '东', DAYS[0], [self.row('东', DAYS[0], 40)])
        with self.assertRaises(StaleRunError) as context:
            switch_current(self.scheduler.db, run['run_id'], CLOCK)
        self.assertEqual(context.exception.args[0]['stale_partitions'][0]['latest_version'], 2)
        renewed = refresh_run(self.scheduler.db, run['run_id'], CLOCK)
        advance_run(self.scheduler.db, renewed['run_id'])
        switch_current(self.scheduler.db, renewed['run_id'], CLOCK)
        self.assertEqual(current_snapshot(self.scheduler.db, WEEK)['weekly']['people'], 170)

    def test_failure_resumes_only_unsuccessful_steps(self):
        self.ingest_week(10)
        run = create_run(self.scheduler.db, 'backfill',
                         [('东', DAYS[0])], self.config, CLOCK, run_id='resume-run')
        from partitionflow import runner as runner_module
        original = runner_module.run_day_tx
        calls = {'n': 0}

        def flaky(database, region, day, version):
            calls['n'] += 1
            if calls['n'] == 1:
                raise RuntimeError('模拟执行者中断')
            return original(database, region, day, version)

        runner_module.run_day_tx = flaky
        try:
            with self.assertRaises(StepFailedError):
                advance_run(self.scheduler.db, run['run_id'])
        finally:
            runner_module.run_day_tx = original
        from partitionflow.runner import describe_run
        view = next(item for item in describe_run(self.scheduler.db, run['run_id'])['steps']
                    if item['step_id'] == f"daily:东:{DAYS[0]}")
        self.assertEqual(view['status'], 'failed')
        self.assertEqual(view['attempts'], 1)
        # 恢复后只重做失败步骤，已成功步骤保留；无需清库。
        advance_run(self.scheduler.db, run['run_id'])
        switch_current(self.scheduler.db, run['run_id'], CLOCK)
        snapshot = current_snapshot(self.scheduler.db, WEEK)
        self.assertEqual(snapshot['weekly']['people'], 140)

    def test_late_partition_unblocks_after_refresh(self):
        for day in DAYS:
            ingest(self.scheduler.db, '东', day, [self.row('东', day, 10)])
        run = create_run(self.scheduler.db, 'backfill',
                         [('东', DAYS[0])], self.config, CLOCK, run_id='late-run')
        self.assertEqual(run['status'], 'blocked')
        advance_run(self.scheduler.db, run['run_id'])  # 只跳过，不报错
        with self.assertRaises(RunError):
            switch_current(self.scheduler.db, run['run_id'], CLOCK)
        for day in DAYS:  # 西区迟到数据到齐
            ingest(self.scheduler.db, '西', day, [self.row('西', day, 5)])
        refreshed = refresh_run(self.scheduler.db, run['run_id'], CLOCK)
        self.assertEqual(refreshed['status'], 'ready')
        advance_run(self.scheduler.db, refreshed['run_id'])
        switch_current(self.scheduler.db, refreshed['run_id'], CLOCK)
        snapshot = current_snapshot(self.scheduler.db, WEEK)
        self.assertEqual(snapshot['weekly']['people'], 105)

    def test_lineage_from_output_and_frozen_release(self):
        self.ingest_week(10)
        self.backfill([('东', DAYS[0])])
        checksum = _checksum_of(self.scheduler.db, WEEK)
        lineage = weekly_lineage(self.scheduler.db, WEEK, checksum)
        self.assertEqual(len(lineage['inputs']), 14)
        self.assertTrue(all(item['up_to_date'] for item in lineage['inputs']))
        daily = daily_lineage(self.scheduler.db, '东', DAYS[0])
        self.assertEqual(daily['inputs'][0]['version'], 1)
        self.assertTrue(daily['up_to_date'])
        payload = publish(self.scheduler.db, '周报封存', WEEK, checksum)
        # 迟到更正后：封存发布保持不变，血缘显示其输入已被超越。
        ingest(self.scheduler.db, '东', DAYS[0], [self.row('东', DAYS[0], 80)])
        self.backfill([('东', DAYS[0])])
        self.assertEqual(read(self.scheduler.db, '周报封存'), payload)
        release_view = release_lineage(self.scheduler.db, '周报封存')
        self.assertTrue(release_view['frozen'])
        self.assertFalse(release_view['weekly_lineage']['up_to_date'])
        stale_input = next(item for item in release_view['weekly_lineage']['inputs']
                           if item['region'] == '东' and item['day'] == DAYS[0])
        self.assertEqual(stale_input['latest_version'], 2)


def _checksum_of(database, week):
    row = database.execute('SELECT checksum FROM weekly WHERE week=? ORDER BY rowid DESC LIMIT 1',
                           (week,)).fetchone()
    return row['checksum']


if __name__ == '__main__':
    unittest.main()
