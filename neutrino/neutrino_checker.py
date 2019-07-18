#!/usr/bin/env python

# Reason for not using ubermon directly:
# - requires a rollout/deployment of the ubermon package, and sometimes the docker hosts have an older version
# - internal helper files like presto_jmx_metrics weren't exposed
# - ubermon presto package is very tied to bare metal
# - refactoring can fix both of these

# Basic differences with regular presto: 
# - We have a multi-coordinator set up, so each node.environment and node.id is actually unique. We disregard that

# TODO: The metric definitions here are the real secret sauce and they should not be duplicated here.

# This file is an amalgamation of uber-metrics binary, presto_jvm.py, presto_metrics.py

# TODO: DRY this with ubermon

import argparse
import collections
import cStringIO
import json
import logging
from logging.handlers import RotatingFileHandler
import re
import requests
from requests.adapters import HTTPAdapter
import socket
import threading
import sys
import time
import urllib3
from six import string_types

METRICS = [
{
    "prefix": "jvm.Memory",
    "mbean": "java.lang:type=Memory",
    "metrics": [('HeapMemoryUsage', ['committed', 'init', 'max', 'used'], 'GAUGE'), ('NonHeapMemoryUsage', ['committed', 'init', 'max', 'used'], 'GAUGE')]
},
{
    "prefix": "jvm.Memory.G1YoungGen",
    "mbean": "java.lang:name=G1 Young Generation,type=GarbageCollector",
    "metrics": [('CollectionCount', None, 'COUNTER'), ('CollectionTime', None, 'COUNTER')]
},
{
    "prefix": "jvm.Memory.G1OldGen",
    "mbean": "java.lang:name=G1 Old Generation,type=GarbageCollector",
    "metrics": [('CollectionCount', None, 'COUNTER'), ('CollectionTime', None, 'COUNTER'), ('LastGcInfo', ['duration'], 'GAUGE')]
},
{
    "prefix": "jvm.Threading",
    "mbean": "java.lang:type=Threading",
    "metrics": ['DaemonThreadCount', 'PeakThreadCount', 'ThreadCount', ('TotalStartedThreadCount', None, 'COUNTER')]
},
{
    "prefix": "presto.Execution",
    "mbean": "com.facebook.presto.execution:name=QueryManager",
},
{
    "prefix": "presto.scheduler",
    "mbean": "com.facebook.presto.execution.scheduler:name=SplitSchedulerStats"
},
{
    "prefix": "presto.httpclient.scheduler",
    "mbean": "io.airlift.http.client:name=ForScheduler,type=HttpClient"
},
{
    "prefix": "presto.metadata",
    "mbean": "com.facebook.presto.metadata:name=DiscoveryNodeManager",
    "metrics": ['activenodecount', 'inactivenodecount', 'shuttingdownnodecount']
},
{
    "prefix": "presto.schedulerclient",
    "mbean": "io.airlift.http.client:type=HttpClient,name=ForScheduler",
    "metrics": ["activeconnectionsperdestination", "connectionstats", "threadpool"]
},
{
    "prefix": "presto.Execution",
    "mbean": "com.facebook.presto.execution:name=TaskManager"
},
{
    "prefix": "presto.Execution",
    "mbean": "com.facebook.presto.execution.executor:name=TaskExecutor",
    "metrics": ["BlockedSplits", "WaitingSplits", "RunnerThreads", "RunningSplits",
                "Tasks", "TotalSplits", "RunAwaySplitCount"]
},
{
    "prefix": "presto.taskresource",
    "mbean": "com.facebook.presto.server:name=TaskResource"
},
{
    "prefix": "presto.httpserver",
    "mbean": "io.airlift.http.server:name=HttpServer",
    "metrics": ["httpconnectionstats"]
},
{
    "prefix": "presto.requeststats",
    "mbean": "io.airlift.http.server:name=RequestStats"
},
{
    "prefix": "presto.statementresource",
    "mbean": "com.facebook.presto.server.protocol:name=StatementResource"
},
{
    "prefix": "presto.exchangeclient",
    "mbean": "io.airlift.http.client:type=HttpClient,name=ForExchange",
    "metrics": ["activeconnectionsperdestination", "connectionstats", "threadpool"]
},
{
    "prefix": "presto.worker.system",
    "mbean": "java.lang:type=OperatingSystem",
    "metrics": ["SystemLoadAverage", "ProcessCpuLoad", "TotalPhysicalMemorySize",
                "FreePhysicalMemorySize", "TotalSwapSpaceSize", "FreeSwapSpaceSize",
                "OpenFileDescriptorCount"]
},
{
    "prefix": "presto.worker.runtime",
    "mbean": "java.lang:type=Runtime",
    "metrics": ["Uptime"]
},
{
    "prefix": "presto.sql.planner.cachingplanner",
    "mbean": "com.facebook.presto.sql.planner:name=CachingPlanner",
    "metrics": ["CachedPlanCalls", "NonCachedPlanCalls"]
},
{
    "prefix": "presto.sql.planner.optimizations.planoptimizer",
    "mbean_prefix": "com.facebook.presto.sql.planner.optimizations:name=PlanOptimizer"
},
{
    "prefix": "presto.sql.planner.optimizations.iterativeoptimizer",
    "mbean_prefix": "com.facebook.presto.sql.planner.iterative:name=IterativeOptimizer"
},
{
    "prefix": "presto.rta.aresdb",
    "mbean_prefix": "com.facebook.presto.aresdb:type=AresDbMetrics"
},
{
    "prefix": "presto.rta.pinot",
    "mbean_prefix": "com.facebook.presto.pinot:type=PinotMetrics"
},
{
    "prefix": "presto.rta.rta",
    "mbean_prefix": "com.facebook.presto.rta:type=RtaMetrics"
},
{
    "prefix": "presto.aresdb.pagesourceprovider",
    "mbean_prefix": "com.facebook.presto.aresdb:type=AresDbPageSourceProvider"
}
]

def compile_metrics(metrics):
    mbeans = {}
    prefix_mbeans = {}
    for m in metrics:
        mbean = m.get('mbean')
        mbean_prefix = m.get('mbean_prefix')
        if mbean:
            mbeans[mbean] = m
        elif mbean_prefix:
            prefix_mbeans[mbean_prefix] = (m, [])

    return (mbeans, prefix_mbeans)

def setup_logging(args):
    global app_log
    fmt = '%(asctime)s %(levelname)s %(message)s'

    if args.stderr_logging:
        logging.basicConfig(
                stream=sys.stderr,
                level=logging.DEBUG,
                format=fmt)
        app_log = logging.getLogger('root')
        app_log.setLevel(logging.DEBUG)
    else:
        app_log = logging.getLogger('root')
        my_handler = RotatingFileHandler(args.log_file, mode='a', maxBytes=1024*1024,
                backupCount=2, encoding=None, delay=0)
        my_handler.setFormatter(logging.Formatter(fmt))
        app_log.setLevel(logging.DEBUG if args.debug else logging.WARNING)
        app_log.addHandler(my_handler)

def filter_none(result):
    if not result:
        return

    for metric_name in result.keys():
        if result[metric_name]['value'] is None or result[metric_name]['value'] == 'NaN':
            result.pop(metric_name)

def construct_regex(ll):
    return re.compile('|'.join(str(l).lower() for l in ll))

DEFAULT_TYPE_MATCHER = construct_regex(['double', 'long', 'int'])

def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = urllib3.util.retry.Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

ALL_TIME_SERIES = construct_regex(['OneMinute', 'FiveMinute', 'FifteenMinute', 'AllTime'])

def get_all_jmx_metrics(port):
    ts = time.time()
    url = 'http://127.0.0.1:' + str(port) + '/v1/jmx/mbean'
    try:
        response = requests_retry_session().get(url, timeout=5)
    except Exception as x:
        app_log.error("Error fetching %s", url)
        return {}

    if not response.ok:
        app_log.error("Error fetching %s: return code is %s", url, response.status_code)
        return {}

    if not response.text:
        app_log.warn("Response fetching %s is empty", url)
        return {}

    ret = {}
    for obj in response.json():
        object_name = obj.get('objectName')
        if not object_name:
            next
        interested = object_name in metrics_per_mbean
        for k, v in prefix_mbeans.iteritems():
            if object_name.startswith(k):
                v[1].append(object_name)
                interested = True
        if interested:
            ret[object_name] = obj
    # if app_log.isEnabledFor(logging.DEBUG):
    #     app_log.debug('All the metrics at time ' + str(int(ts)))
    #     for k, v in ret.iteritems():
    #         app_log.debug('{} = {}'.format(k, json.dumps(v)))
    return (ret, ts)

def get_metrics_helper(all_metrics, metric_name, metrics, ts):
    prefix = metrics['prefix']
    mbean_object = all_metrics.get(metric_name)
    if not mbean_object:
        return {}

    mbean_prefix = metrics.get('mbean_prefix', None)
    if mbean_prefix and metric_name.startswith(mbean_prefix):
        for extra_word in metric_name[len(mbean_prefix):].split(','):
            extra_word = extra_word.strip()
            if extra_word:
                k, v = extra_word.split('=')
                prefix += '.' + v

    desired_attributes = {}
    for metric in metrics.get('metrics', []):
        if isinstance(metric, tuple):
            attr, sub_metrics, m3type = metric
            desired_attributes[attr.lower()] = (attr, sub_metrics, m3type)
        elif isinstance(metric, string_types):
            desired_attributes[metric.lower()] = (metric, None, 'GAUGE')

    results = {}
    for attr in mbean_object.get('attributes', []):
        if 'name' not in attr or 'value' not in attr or 'type' not in attr:
            continue

        name = str(attr['name'])
        value = attr['value']
        t = attr['type']

        nl = name.lower()

        attr_details = desired_attributes.get(nl, None)

        if attr_details is None:
            # No metrics were given, get everything for primitives
            if DEFAULT_TYPE_MATCHER.search(t):
                key = ".".join([prefix, name])
                results[key] = {'ts': ts, 'type': 'GAUGE', 'value': value}
        else:
            publish_name = attr_details[0]
            sub_metrics = attr_details[1]
            if sub_metrics is None and DEFAULT_TYPE_MATCHER.search(t):
                # valid primitive type
                key = ".".join([prefix, publish_name])
                results[key] = {'ts': ts, 'type': attr_details[2], 'value': value}
            elif sub_metrics and isinstance(value, collections.Mapping):
                for sub_metric in sub_metrics:
                    value_sub_metric = value.get(sub_metric, None)
                    if value_sub_metric:
                        key = ".".join([prefix, publish_name, sub_metric])
                        results[key] = {'ts': ts, 'type': attr_details[2], 'value': value_sub_metric}
    return results

def run_check(port, service_name, m3obj, debug):
    result = {}
    all_metrics, ts = get_all_jmx_metrics(port)
    for metric_name, metrics in metrics_per_mbean.iteritems():
        result.update(get_metrics_helper(all_metrics, metric_name, metrics, ts))
    for prefix_mbeans_value in prefix_mbeans.values():
        metrics = prefix_mbeans_value[0]
        for metric_name in prefix_mbeans_value[1]:
            result.update(get_metrics_helper(all_metrics, metric_name, metrics, ts))

    filter_none(result)
    if debug:
        app_log.info('Data for graphite @ ' + str(ts))
        to_dump = m3obj.format_for_graphite(service_name, result)
        app_log.info(to_dump)
    m3obj.update(service_name, result)
    return ts

class FakeMetricsObject:
    def __init__(self):
        try:
            hostname = socket.gethostname()
        except:
            hostname = 'fake_hostname'
        self.hostnames = [hostname]

    def update(self, service_name, result):
        pass

    # taken from the real ubermon/__init__.py
    def format_for_graphite(self, service_name, metrics, log_meta_metrics=True):
        data = cStringIO.StringIO()
        app_log.info('Publishing for the name {} and {} with len {}'.format(service_name, type(metrics), len(metrics)))
        for name, v in metrics.iteritems():
            if name.startswith('cluster_metric.'):
                names = [name]
            else:
                names = []
                for host in self.hostnames:
                    names.append('servers.%s.%s.%s' % (host, service_name, name))
            for name in names:
                line = '%s %f %i\n' % (
                    name,
                    v['value'],
                    int(v['ts']))
                data.write(line)
        return data.getvalue()

class RealUberMonWrapper:
    def __init__(self, config_path):
        import ubermon
        self.ubermon = ubermon
        self.ubermon.init(config_path)

    def create_m3_obj(self):
        m3obj = self.ubermon.Metrics()
        m3obj.current_check_group = 'neutrino'
        return m3obj

    def get_config(self, conf, default = None):
        return self.ubermon.conf(conf, default=20)

class FakeUberMonWrapper:
    def create_m3_obj(self):
        self.config = {'check_timeout' : 10}
        return FakeMetricsObject()

    def get_config(self, conf, default = None):
        return self.config.get(conf, default)

def run_checks(port, service_name, debug=False):
    m3obj = ubermon_wrapper.create_m3_obj()

    t0 = time.time()
    ret = None
    try:
        ret = run_check(port, service_name, m3obj, debug)
    except Exception, e:
        app_log.exception(e)
    app_log.debug('completed in %0.2f seconds\n' % (time.time() - t0))
    return ret

def daemon_checks(right_away, port, service_name, interval=60, debug=False):
    ticker = threading.Event()
    next_interval = 0 if right_away else interval
    while not ticker.wait(next_interval):
        check_run_time = run_checks(port, service_name, debug)
        next_interval = interval if check_run_time is None else max(check_run_time + interval - time.time(), 0)


app_log = None
metrics_per_mbean = None
prefix_mbeans = None
ubermon_wrapper = None

def main():
    global ubermon_wrapper
    global metrics_per_mbean
    global prefix_mbeans

    metrics_per_mbean, prefix_mbeans = compile_metrics(METRICS)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c',
        '--conf-path',
        default='/etc/uber/metrics.conf',
        help='Path to config file (default %(default)s)'
    )
    parser.add_argument(
        '-l',
        '--log_file',
        default='metrics.log',
        help='Path to metrics file'
    )
    parser.add_argument(
        '--interval',
        default=60,
        type=float,
        help='interval for daemon checks'
    )
    parser.add_argument(
        '--port',
        default=8080,
        type=int,
        help='Port'
    )
    parser.add_argument(
        '-s',
        '--service-name',
        required=True,
        help='Service name'
    )
    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Debug'
    )
    parser.add_argument(
        '--stderr_logging',
        action='store_true',
        help='Debug'
    )
    parser.add_argument(
        '--right-away',
        action='store_true',
        help='Do the first round right away'
    )
    parser.add_argument(
        '--fake_ubermon',
        action='store_true',
        help='Run with a fake ubermon'
    )
    args = parser.parse_args()
    ubermon_wrapper = RealUberMonWrapper(args.conf_path) if not args.fake_ubermon else FakeUberMonWrapper()
    setup_logging(args)
    daemon_checks(
        right_away=args.right_away,
        port=args.port,
        service_name=args.service_name,
        interval=args.interval,
        debug=args.debug
    )

if __name__ == '__main__':
    main()
