from fabric_cf.orchestrator.swagger_server.models.resources import Resources  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import resources_controller as rc


def portalresources_get(graph_format, level=None, force_refresh=None, start_date=None, end_date=None, includes=None, excludes=None):  # noqa: E501
    """Retrieve a listing and description of available resources for portal

    Retrieve a listing and description of available resources for portal # noqa: E501

    :param graph_format: graph format
    :type graph_format: str
    :param level: Level of details
    :type level: int
    :param force_refresh: Force to retrieve current available resource information.
    :type force_refresh: bool
    :param start_date: starting date to check availability from
    :type start_date: str
    :param end_date: end date to check availability until
    :type end_date: str
    :param includes: comma separated lists of sites to include
    :type includes: str
    :param excludes: comma separated lists of sites to exclude
    :type excludes: str

    :rtype: Resources
    """
    return rc.portalresources_get(graph_format=graph_format, level=level, force_refresh=force_refresh,
                                  start_date=start_date, end_date=end_date, includes=includes, excludes=excludes)


def resources_get(level, force_refresh, start_date=None, end_date=None, includes=None, excludes=None):  # noqa: E501
    """Retrieve a listing and description of available resources. By default, a cached available resource information is returned. User can force to request the current available resources.

    Retrieve a listing and description of available resources. By default, a cached available resource information is returned. User can force to request the current available resources. # noqa: E501

    :param level: Level of details
    :type level: int
    :param force_refresh: Force to retrieve current available resource information.
    :type force_refresh: bool
    :param start_date: starting date to check availability from
    :type start_date: str
    :param end_date: end date to check availability until
    :type end_date: str
    :param includes: comma separated lists of sites to include
    :type includes: str
    :param excludes: comma separated lists of sites to exclude
    :type excludes: str

    :rtype: Resources
    """
    return rc.resources_get(level=level, force_refresh=force_refresh, start_date=start_date,
                            end_date=end_date, includes=includes, excludes=excludes)


def portalresources_summary_get(level=None, force_refresh=None, start_date=None, end_date=None,
                                 includes=None, excludes=None, type=None):  # noqa: E501
    """Retrieve a JSON summary of available resources for portal"""
    return rc.portalresources_summary_get(level=level, force_refresh=force_refresh,
                                           start_date=start_date, end_date=end_date,
                                           includes=includes, excludes=excludes, type=type)


def resources_summary_get(level=None, force_refresh=None, start_date=None, end_date=None,
                           includes=None, excludes=None, type=None):  # noqa: E501
    """Retrieve a JSON summary of available resources"""
    return rc.resources_summary_get(level=level, force_refresh=force_refresh,
                                     start_date=start_date, end_date=end_date,
                                     includes=includes, excludes=excludes, type=type)


def resources_calendar_get(start_date, end_date, interval=None, site=None, host=None,
                            exclude_site=None, exclude_host=None):  # noqa: E501
    """Retrieve resource availability calendar

    Returns resource availability over time slots, proxied from the reports API. # noqa: E501

    :param start_date: Start time for the calendar range (ISO 8601)
    :type start_date: str
    :param end_date: End time for the calendar range (ISO 8601)
    :type end_date: str
    :param interval: Time interval (day or week)
    :type interval: str
    :param site: Filter by site
    :type site: List[str]
    :param host: Filter by host
    :type host: List[str]
    :param exclude_site: Exclude sites
    :type exclude_site: List[str]
    :param exclude_host: Exclude hosts
    :type exclude_host: List[str]

    :rtype: Resources
    """
    return rc.resources_calendar_get(start_date=start_date, end_date=end_date, interval=interval,
                                      site=site, host=host, exclude_site=exclude_site,
                                      exclude_host=exclude_host)


def resources_find_slot(body):  # noqa: E501
    """Find available time slots for resources

    Find the earliest time windows where all requested resources are simultaneously available. # noqa: E501

    :param body: Resource request payload
    :type body: dict

    :rtype: Resources
    """
    return rc.resources_find_slot(body=body)
