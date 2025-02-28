from restful_gateway import RestfulGateway
from metric_collector import MetricCollector

def evaluate_others(
        restful_gateway: RestfulGateway,
        metric_collector: MetricCollector,
    ):
    do_not_scale = restful_gateway.config_gateway.do_not_scale
    if not do_not_scale:
        restful_gateway.wait_for_scaling()
    restful_gateway.wait_for_canceling(metric_collector)
    
    