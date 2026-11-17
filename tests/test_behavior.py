import tempfile
import unittest


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.directory=self.temporary.name
        self.resources=[]

    def open(self, factory):
        resource=factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()

from partitionflow.publication import Scheduler, publish, read
from partitionflow.engine import ingest, run_day, run_week
from partitionflow.calendar import business_day, week_of, date_map


class Behavior(WorkspaceTest):
    def test_calendar(self):
        self.assertEqual(business_day('2026-10-31T18:00:00+00:00','Asia/Shanghai'),'2026-11-01')
        self.assertEqual(week_of('2026-11-01'),'2026-10-26')
        self.assertTrue(date_map('2026-11-01','Asia/Shanghai')['utc_start'].startswith('2026-10-31'))
        with self.assertRaises(ValueError):
            business_day('2026-11-01T01:00:00','UTC')

    def test_partition_versions(self):
        scheduler=self.open(Scheduler)
        row={'region':'东','day':'2026-11-01','people':4}
        self.assertEqual(ingest(scheduler.db,'东','2026-11-01',[row]),1)
        self.assertEqual(ingest(scheduler.db,'东','2026-11-01',[row]),1)
        self.assertEqual(ingest(scheduler.db,'东','2026-11-01',[dict(row,people=5)]),2)
        self.assertEqual(run_day(scheduler.db,'东','2026-11-01',1)['people'],4)

    def test_week_requires_inputs(self):
        scheduler=self.open(Scheduler)
        with self.assertRaises(ValueError):
            run_week(scheduler.db,'2026-10-26',[('东','2026-11-01',1)])
        ingest(scheduler.db,'东','2026-11-01',[{'region':'东','day':'2026-11-01','people':4}])
        run_day(scheduler.db,'东','2026-11-01',1)
        with self.assertRaises(ValueError):
            run_week(scheduler.db,'2026-11-02',[('东','2026-11-01',1)])

    def test_release_frozen(self):
        scheduler=self.open(Scheduler)
        ingest(scheduler.db,'东','2026-11-01',[{'region':'东','day':'2026-11-01','people':4}])
        run_day(scheduler.db,'东','2026-11-01',1)
        checksum=run_week(scheduler.db,'2026-10-26',[('东','2026-11-01',1)])
        old=publish(scheduler.db,'周报','2026-10-26',checksum)
        self.assertEqual(old['people'],4)
        reopened=self.open(Scheduler)
        self.assertEqual(read(reopened.db,'周报'),old)
