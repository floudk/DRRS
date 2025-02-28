from restful_gateway import RestfulGateway
from metric_collector import MetricCollector

from subscale.min_hold_scheduler import MinHoldScheduler


def evaluate_drrs(
        restful_gateway: RestfulGateway,
        metric_collector: MetricCollector,
    ):
    scheduler = MinHoldScheduler(restful_gateway, metric_collector, subscale_min_group_num=7, conflict_concurrent=2)
    scheduler.run()



