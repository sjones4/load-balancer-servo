# Copyright 2020 AppScale Systems, Inc
#
# Use of this source code is governed by a BSD-2-Clause
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/BSD-2-Clause
import argparse
import boto3
import hashlib
import json
import logging
import os
import re
import redis
import servo.config as config
import time

from botocore.client import Config
from botocore.exceptions import ClientError
from logging.handlers import RotatingFileHandler


JEXCEPT_CLASS = 'com.eucalyptus.loadbalancing.workflow.LoadBalancingActivityException'
ACTIVITY_CHANNELS = {
    'LoadBalancingVmActivities.getCloudWatchMetrics': 'get-cloudwatch-metrics',
    'LoadBalancingVmActivities.getInstanceStatus': 'get-instance-status',
    'LoadBalancingVmActivities.setLoadBalancer': 'set-loadbalancer',
    'LoadBalancingVmActivities.setPolicy': 'set-policy',
}
ACTIVITY_DEFAULT_VALUES = {
    'LoadBalancingVmActivities.getCloudWatchMetrics': 'GetCloudWatchMetrics',
    'LoadBalancingVmActivities.getInstanceStatus': 'GetInstanceStatus',
}
ACTIVITY_LAST_VALUES = {
    'LoadBalancingVmActivities.setLoadBalancer': '',
    'LoadBalancingVmActivities.setPolicy': '',
}
ACTIVITY_VALUES_BY_SHA1 = {
    'LoadBalancingVmActivities.setLoadBalancer': {},
    'LoadBalancingVmActivities.setPolicy': {},
}
ACTIVITY_VERSION = '1.0'
ACTIVITY_CACHE_SECONDS = 300


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True,
                        help="SWF Service Endpoint")
    parser.add_argument("-d", "--domain", required=True,
                        help="SWF Domain")
    parser.add_argument("-l", "--tasklist", required=True,
                        help="SWF task list")
    parser.add_argument("-o", "--timeout", type=int, default=30,
                        help="SWF client connection timeout")
    parser.add_argument("-m", "--maxconn", type=int, default=1,
                        help="SWF client max connections")
    parser.add_argument("-r", "--retention", type=int, default=1,
                        help="SWF domain retention period in days")
    parser.add_argument("-t", "--threads", type=int, default=1,
                        help="Polling threads count (ignored)")
    parser.add_argument("--jar", help="JAR file that implement workflows and activities (ignored)")
    parser.add_argument("--classes", help="Limit workflow and activities classes to load "
                        "(class names are separated by ':') (ignored)")
    parser.add_argument("--credential", help="Property file containing AWS credentials to use "
                        "(default is to use session credentials from instance's metadata")
    parser.add_argument("--loglevel", default="INFO",
                        help="Logging level (default: INFO)")
    parser.add_argument("--logdir", default="/var/log/load-balancer-servo",
                        help="Directory containing log files (default: /var/log/eucalyptus)")
    parser.add_argument("--logappender", default="console-log",
                        help="Log4j appender to use (ignored)")
    return parser


def init_logging(args):
    global log
    LOG_FILE = '{}/servo-workflow.log'.format(args.logdir)
    LOG_BYTES = 1024 * 1024  # 1MB
    LOG_FORMAT = "%(asctime)s %(name)s [%(levelname)s]:%(message)s"
    LOG_HANDLER = RotatingFileHandler(LOG_FILE, maxBytes=LOG_BYTES, backupCount=5)
    LOG_HANDLER.setFormatter(logging.Formatter(LOG_FORMAT))
    log = logging.getLogger('servo-workflow')
    log.setLevel(getattr(logging, args.loglevel.upper(), logging.INFO))
    log.addHandler(LOG_HANDLER)


def log_debug(message, *args):
    log.debug(message, *args)


def log_info(message, *args):
    log.info(message, *args)


def build_client(args):
    log_info('Using endpoint %s', args.endpoint)

    boto_config = Config(region_name='eucalyptus',
                         connect_timeout=args.timeout,
                         read_timeout=50,
                         max_pool_connections=args.maxconn)

    swf = boto3.client('swf',
                       config=boto_config,
                       use_ssl=False,
                       endpoint_url=args.endpoint)
    return swf


def register_activities(args, swf):
    for activity_name in ACTIVITY_CHANNELS:
        try:
            swf.register_activity_type(domain=args.domain,
                                       name=activity_name,
                                       version=ACTIVITY_VERSION,
                                       description='',
                                       defaultTaskHeartbeatTimeout='NONE',
                                       defaultTaskStartToCloseTimeout='60',
                                       defaultTaskScheduleToStartTimeout='60',
                                       defaultTaskScheduleToCloseTimeout='120')
            log_info('Registered activity type %s %s', activity_name, ACTIVITY_VERSION)
        except swf.exceptions.TypeAlreadyExistsFault:
            log_info('Activity type already exists %s %s', activity_name, ACTIVITY_VERSION)


def do_activity_polling(args, swf):
    log_info('Polling for tasks')
    while True:
        try:
            task = swf.poll_for_activity_task(
                      domain=args.domain,
                      taskList={'name': args.tasklist},
                      identity='client-worker-{}'.format(args.tasklist))
        except KeyboardInterrupt:
            log_info('Exiting due to CTRL-C')
            logging.shutdown()
            break
        except ClientError as error:
            log_info('Exiting due to: %s"', str(error))
            logging.shutdown()
            break

        if 'taskToken' in task:
            task_token = task['taskToken']
            task_activity = task['activityType']['name']
            task_params = json.loads(task['input'])[1]
            try:
                task_result = do_activity(task_activity, task_params)
                log_debug('Activity task completed: %s', task_result)
                swf.respond_activity_task_completed(
                     taskToken=task_token,
                     result=json.dumps(task_result))
            except Exception as ex:
                log_debug('Activity task failed: %s', str(ex))
                swf.respond_activity_task_failed(
                    taskToken=task_token,
                    reason=str(ex),
                    details=json.dumps([JEXCEPT_CLASS, {'message': str(ex)}]))


def do_activity(activity, params):
    log_debug('Handling activity %s with params %s', activity, str(params))
    value = params[0] if len(params) > 0 else ACTIVITY_DEFAULT_VALUES[activity]
    if activity in ACTIVITY_LAST_VALUES:
        value = activity_value_cache(activity, value)
        if value != ACTIVITY_LAST_VALUES[activity]:
            log_info('Activity %s value updated', activity)
            ACTIVITY_LAST_VALUES[activity] = value
            store_activity_value(ACTIVITY_CHANNELS[activity][4:], value)
    sred = redis.StrictRedis(host='localhost', port=6379)
    sred.publish(ACTIVITY_CHANNELS[activity], value)
    if not len(params):
        return sred.blpop('{}-reply'.format(ACTIVITY_CHANNELS[activity]), 0)[1]
    else:
        return None


def store_activity_value(name, value):
    value_file = os.path.join(config.DEFAULT_PID_ROOT, '{}.xml'.format(name))
    with open(value_file, 'w') as f:
        f.write(value)


def activity_value_cache(activity, value):
    value_cache = ACTIVITY_VALUES_BY_SHA1[activity]
    value_sha1 = value
    if len(value) == 40 and re.match('[0-9a-fA-F]{40}', value):
        log_debug('Using cached value for activity %s with key %s', activity, value)
        _, value = value_cache[value]
    else:
        sha1 = hashlib.sha1()
        sha1.update(value.encode('utf-8'))
        value_sha1 = sha1.hexdigest()
        log_debug('Cached value for activity %s with key %s', activity, value_sha1)
    time_now = time.time()
    value_cache[value_sha1] = (time_now, value)
    cache_maintain(activity, time_now)
    return value


def cache_maintain(activity, time_now):
    value_cache = ACTIVITY_VALUES_BY_SHA1[activity]
    stale_keys = set()
    for key, (timestamp, value) in value_cache.items():
        if time_now > (timestamp + ACTIVITY_CACHE_SECONDS):
            stale_keys.add(key)
    for stale_key in stale_keys:
        log_debug('Clearing stale cached value for activity %s with key %s',
                  activity, stale_key)
        del value_cache[stale_key]


def main():
    parser = build_parser()
    args = parser.parse_args()
    init_logging(args)
    swf = build_client(args)
    register_activities(args, swf)
    do_activity_polling(args, swf)


if __name__ == "__main__":
    main()
