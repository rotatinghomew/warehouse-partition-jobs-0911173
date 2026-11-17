from .publication import Scheduler, publish
from .engine import ingest, run_day, run_week
from .calendar import date_map


def run(root):
    scheduler=Scheduler(root)
    try:
        version=ingest(scheduler.db,'东','2026-11-01',[{'region':'东','day':'2026-11-01','people':12}])
        run_day(scheduler.db,'东','2026-11-01',version)
        checksum=run_week(scheduler.db,'2026-10-26',[('东','2026-11-01',version)])
        return {'业务日映射':date_map('2026-11-01','Asia/Shanghai'),'发布':publish(scheduler.db,'周度摘录','2026-10-26',checksum)}
    finally:
        scheduler.close()
