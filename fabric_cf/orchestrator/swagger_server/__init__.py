import logging

import prometheus_client
received_counter = prometheus_client.Counter('Requests_Received', 'HTTP Requests', ['method', 'endpoint'])
success_counter = prometheus_client.Counter('Requests_Success', 'HTTP Success', ['method', 'endpoint'])
failure_counter = prometheus_client.Counter('Requests_Failed', 'HTTP Failures', ['method', 'endpoint'])

__API_REFERENCE__ = 'https://github.com/fabric-testbed/ControlFramework'
