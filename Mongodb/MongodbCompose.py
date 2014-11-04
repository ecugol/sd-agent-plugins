"""
Server Density Plugin
MongodbCompose

https://www.serverdensity.com/plugins/mongodb/
https://github.com/serverdensity/sd-agent-plugins/


Version: 1.0.0
"""

import collections
import datetime
import traceback
import urllib2
import json


try:
    from pymongo import MongoClient
except ImportError:
    pass


COMPOSE_API_BASE = 'https://api.compose.io'
COMPOSE_API_SERVER_STATUS = COMPOSE_API_BASE + \
    '/deployments/{}/{}/mongodb/server-status'
COMPOSE_API_LIST_DATABSES = COMPOSE_API_BASE + \
    '/deployments/{}/{}/databases'
COMPOSE_API_DBSTATS = COMPOSE_API_BASE + \
    '/deployments/{}/{}/mongodb/{}/stats'


def flatten(dictionary, parent_key='', sep='_'):
    """Code snipped taken from
       http://stackoverflow.com/questions/6027558/\
       flatten-nested-python-dictionaries-compressing-keys
       to "flattern" a nested dict.
    """

    items = []
    for key, value in dictionary.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)


class MongodbCompose(object):
    """Plugin class to manage extracting the data from Mongo
       for the sd-agent.
    """

    def __init__(self, agent_config, checks_logger, raw_config):
        self.agent_config = agent_config
        self.checks_logger = checks_logger
        self.raw_config = raw_config
        self.mongo_DB_store = None
        self.connection = None
        self.compose_dbname = None
        self.compose_account = None
        self.compose_deployment = None
        self.compose_token = None

    def preliminaries(self):
        if ('MongoDB' not in self.raw_config
                or 'mongodb_plugin_server' not in self.raw_config['MongoDB']
                or self.raw_config['MongoDB']['mongodb_plugin_server'] == ''):
            self.checks_logger.debug('mongodb_plugin: config not set')
            return False

        # Checks that all necessary config fields are exist:
        required_fields = (
            'mongodb_plugin_compose_dbname',
            'mongodb_plugin_compose_token',
            'mongodb_plugin_compose_account',
            'mongodb_plugin_compose_deployment',

        )

        for field in required_fields:
            if not self.raw_config['MongoDB'].get(field, None):
                self.checks_logger.error((
                    'mongodb_plugin: config not set. {} is required'
                    ' if mongodb_plugin_compose is `yes`'.format(field)
                ))
                return False

        self.compose_dbname = \
            self.raw_config['MongoDB']['mongodb_plugin_compose_dbname']
        self.compose_account = \
            self.raw_config['MongoDB']['mongodb_plugin_compose_account']
        self.compose_deployment = \
            self.raw_config['MongoDB']['mongodb_plugin_compose_deployment']
        self.compose_token = \
            self.raw_config['MongoDB']['mongodb_plugin_compose_token']

        self.checks_logger.debug('mongodb_plugin: config set')

        try:
            from pymongo import MongoClient
        except ImportError:
            self.checks_logger.error(
                'mongodb_plugin: unable to import pymongo library'
            )
            return False

        return True

    def get_connection(self):
        try:
            import urlparse
            parsed = urlparse.urlparse(
                self.raw_config['MongoDB']['mongodb_plugin_server']
            )

            mongo_uri = ''

            # Can't use attributes on Python 2.4
            if parsed[0] != 'mongodb':

                mongo_uri = 'mongodb://'

                if parsed[2]:

                    if parsed[0]:

                        mongo_uri = mongo_uri + parsed[0] + ':' + parsed[2]

                    else:
                        mongo_uri = mongo_uri + parsed[2]

            else:

                mongo_uri = self.raw_config['MongoDB']['mongodb_plugin_server']

            self.checks_logger.debug('-- mongo_uri: %s', mongo_uri)

            self.connection = MongoClient(
                mongo_uri
            )

            self.checks_logger.debug('Connected to MongoDB')

        except Exception:
            self.checks_logger.error(
                'Unable to connect to MongoDB server %s - Exception = %s',
                mongo_uri,
                traceback.format_exc()
            )
            return False
        return True

    def run(self):
        self.checks_logger.debug('mongodb_plugin: started gathering data')

        if not self.preliminaries():
            return False

        if not self.get_connection():
            return False

        # Connects to the DB
        db = self.connection[self.compose_dbname]

        # Gets server status in
        status_output = self._get_server_status_from_api()

        # Gets checks_down
        checks_down = db.checks.find({'is_up': 0, 'is_paused': 0, 'last_checked_time': {'$gt': 0}}).count()

        # Gets checks per minute
        end = datetime.datetime.utcnow()
        start = end - datetime.timedelta(minutes=5)
        checks_per_minute = db.check_stats.find({"$gte": start, "$lt": end}).count() / 5.0

        result = {}
        for status in status_output:
            if status[1].get('repl', False):
                if status[1]['repl'].get('ismaster', False):
                    result = status[1]
                    break
        result['checks_down'] = checks_down
        result['checks_per_minute'] = checks_per_minute
        return flatten(result)

    def _get_server_status_from_api(self):
        data = self._do_request_compose(COMPOSE_API_SERVER_STATUS)
        return data.items()

    def _get_list_of_databases(self):
        data = self._do_request_compose(COMPOSE_API_LIST_DATABSES)
        return [f['name'] for f in data]

    def _get_dbstats(self, dbname):
        data = self._do_request_compose(
            COMPOSE_API_DBSTATS, extra_args=[dbname, ]
        )
        return data

    def _do_request_compose(self, url, extra_args=None):
        format_args = [self.compose_account, self.compose_deployment]
        if extra_args:
            format_args.extend(extra_args)
        URL = url.format(*format_args)
        headers = {
            'Content-Type': 'application/json',
            'Accept-Version': '2014-06',
            'Authorization': 'Bearer {}'.format(self.compose_token)
        }
        req = urllib2.Request(URL, headers=headers)
        try:
            response = urllib2.urlopen(req)
        except urllib2.HTTPError:
            return {}
        else:
            return json.load(response)
