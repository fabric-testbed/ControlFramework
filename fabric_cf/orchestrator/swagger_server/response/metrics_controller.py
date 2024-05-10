from typing import List

from fabric_cf.orchestrator.swagger_server.response.utils import get_token

from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, METRICS_GET_PATH

from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter

from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler

from fabric_cf.orchestrator.swagger_server.response.cors_response import cors_200, cors_500

from fabric_cf.orchestrator.swagger_server.models import Metrics


def metrics_overview_get(excluded_projects: List[str] = None) -> Metrics:  # noqa: E501
    """Control Framework metrics overview
    {
    "results": [
        {
            "last_updated": "2024-04-02 19:50:00.00+00",
            "slices": {
                "active_cumulative": 164,
                "non_active_cumulative": 0
            }
        }
    ],
    "size": 1,
    "status": 200,
    "type": "metrics.overview"
    }

    :rtype: Metrics
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, METRICS_GET_PATH).inc()
    try:
        token = get_token()
        metrics = handler.get_metrics_overview(token=token, excluded_projects=excluded_projects)
        response = Metrics()
        if metrics:
            if isinstance(metrics, list):
                response.results = metrics
            else:
                response.results = [metrics]
        else:
            response.results = []

        response.size = len(response.results)
        response.status = 200
        response.type = 'metrics.overview'
        success_counter.labels(GET_METHOD, METRICS_GET_PATH).inc()
        return cors_200(response_body=response)
    except Exception as exc:
        details = 'Oops! something went wrong with metrics_overview_get(): {0}'.format(exc)
        logger.error(details)
        failure_counter.labels(GET_METHOD, METRICS_GET_PATH).inc()
        return cors_500(details=details)
