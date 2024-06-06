from fabric_cf.orchestrator.swagger_server.models.metrics import Metrics  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import metrics_controller as rc


def metrics_overview_get(excluded_projects=None):  # noqa: E501
    """Control Framework metrics overview

    Control Framework  metrics overview # noqa: E501

    :param excluded_projects: List of projects to exclude from the metrics overview
    :type excluded_projects: List[str]

    :rtype: Metrics
    """
    return rc.metrics_overview_get(excluded_projects=excluded_projects)
