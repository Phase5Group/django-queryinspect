import logging
import collections
import time
import traceback
import math
import re
from django.conf import settings
from django.db import connection
from django.core.exceptions import MiddlewareNotUsed
from django.utils import termcolors

try:
    from django.db.backends.utils import CursorDebugWrapper
except ImportError:
    from django.db.backends.util import CursorDebugWrapper

if hasattr(logging, 'NullHandler'):
    NullHandler = logging.NullHandler
else:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

log = logging.getLogger(__name__)
log.addHandler(NullHandler())

cfg = dict(
    enabled=(
        settings.DEBUG and
        getattr(settings, 'QUERY_INSPECT_ENABLED', False)),
    log_stats=getattr(settings, 'QUERY_INSPECT_LOG_STATS', True),
    header_stats=getattr(settings, 'QUERY_INSPECT_HEADER_STATS', True),
    log_queries=getattr(settings, 'QUERY_INSPECT_LOG_QUERIES', False),
    log_tbs=getattr(settings, 'QUERY_INSPECT_LOG_TRACEBACKS', False),
    roots=getattr(settings, 'QUERY_INSPECT_TRACEBACK_ROOTS', None),
    stddev_limit=getattr(
        settings, 'QUERY_INSPECT_STANDARD_DEVIATION_LIMIT',
        None),
    absolute_limit=getattr(settings, 'QUERY_INSPECT_ABSOLUTE_LIMIT', None),
    threshold=getattr(settings, 'QUERY_INSPECT_THRESHOLD', {
        'MEDIUM': 3,
        'HIGH': 20,
    }),
    ignore_patterns=getattr(settings, 'QUERY_INSPECT_IGNORE_PATTERNS', []),
)

__all__ = ['QueryInspectMiddleware']


class QueryInspectMiddleware(object):

    class QueryInfo(object):
        __slots__ = ('sql', 'time', 'tb')

    def __init__(self, *args, **kwargs):

        if not cfg['enabled']:
            raise MiddlewareNotUsed()

        # colorizing methods
        self.white = termcolors.make_style(fg='white')
        self.red = termcolors.make_style(fg='red')
        self.yellow = termcolors.make_style(fg='yellow')
        self.green = termcolors.make_style(fg='green')

        super(QueryInspectMiddleware, self).__init__(*args, **kwargs)

    @classmethod
    def patch_cursor(cls):
        real_exec = CursorDebugWrapper.execute
        real_exec_many = CursorDebugWrapper.executemany

        def should_include(path):
            if path == __file__ or path + 'c' == __file__:
                return False
            if not cfg['roots']:
                return True
            else:
                for root in cfg['roots']:
                    if path.startswith(root):
                        return True
                return False

        def tb_wrap(fn):
            def wrapper(self, *args, **kwargs):
                try:
                    return fn(self, *args, **kwargs)
                finally:
                    if hasattr(self.db, 'queries'):
                        tb = traceback.extract_stack()
                        tb = [f for f in tb if should_include(f[0])]
                        self.db.queries[-1]['tb'] = tb

            return wrapper

        CursorDebugWrapper.execute = tb_wrap(real_exec)
        CursorDebugWrapper.executemany = tb_wrap(real_exec_many)

    def get_query_infos(self, queries):
        retval = []
        for q in queries:
            qi = self.QueryInfo()
            qi.sql = q['sql']
            qi.time = float(q['time'])
            qi.tb = q.get('tb')
            retval.append(qi)
        return retval

    @staticmethod
    def count_duplicates(infos):
        buf = collections.defaultdict(lambda: 0)
        for qi in infos:
            buf[qi.sql] = buf[qi.sql] + 1
        return sorted(buf.items(), key=lambda el: el[1], reverse=True)

    @staticmethod
    def group_queries(infos):
        buf = collections.defaultdict(lambda: [])
        for qi in infos:
            buf[qi.sql].append(qi)
        return buf

    def ignore_request(self, path):
        """Check to see if we should ignore the request."""
        return any([
            re.match(pattern, path)
            for pattern in cfg['ignore_patterns']
        ])

    def colorize(self, output, count):
        if count > cfg['threshold']['HIGH']:
            output = self.red(output)
        elif count > cfg['threshold']['MEDIUM']:
            output = self.yellow(output)
        else:
            output = self.green(output)
        return output

    def check_duplicates(self, infos):
        duplicates = [
            (qi, num) for qi, num in self.count_duplicates(infos)
            if num > 1]
        duplicates.reverse()
        n = 0
        if len(duplicates) > 0:
            n = (sum(num for qi, num in duplicates) - len(duplicates))

        dup_groups = self.group_queries(infos)

        if cfg['log_queries']:
            for sql, num in duplicates:
                log.info(
                    self.colorize(
                        '[SQL] repeated query (%dx): %s\n' % (num, sql),
                        num
                    )
                )
                if cfg['log_tbs'] and dup_groups[sql]:
                    log.info(
                        'Traceback:\n' +
                        ''.join(traceback.format_list(dup_groups[sql][0].tb)))

        return n

    def check_stddev_limit(self, infos):
        total = sum(qi.time for qi in infos)
        n = len(infos)

        if cfg['stddev_limit'] is None or n == 0:
            return

        mean = total / n
        stddev_sum = sum(math.sqrt((qi.time - mean) ** 2) for qi in infos)
        if n < 2:
            stddev = 0
        else:
            stddev = math.sqrt((1.0 / (n - 1)) * (stddev_sum / n))

        query_limit = mean + (stddev * cfg['stddev_limit'])

        for qi in infos:
            if qi.time > query_limit:
                log.info(
                    '[SQL] query execution of %d ms over limit of '
                    '%d ms (%d dev above mean): %s' % (
                        qi.time * 1000,
                        query_limit * 1000,
                        cfg['stddev_limit'],
                        qi.sql))

    def check_absolute_limit(self, infos):
        n = len(infos)
        if cfg['absolute_limit'] is None or n == 0:
            return

        query_limit = cfg['absolute_limit'] / 1000.0

        for qi in infos:
            if qi.time > query_limit:
                log.info(
                    '[SQL] query execution of %d ms over absolute '
                    'limit of %d ms: %s' % (
                        qi.time * 1000,
                        query_limit * 1000,
                        qi.sql))

    def output_stats(self, infos, num_duplicates, request_time, response):
        sql_time = sum(qi.time for qi in infos)
        n = len(infos)

        if cfg['log_stats']:
            log.info(self.yellow(
                '[SQL] %d queries (%d duplicates), %d ms SQL time, '
                '%d ms total request time' % (
                    n,
                    num_duplicates,
                    sql_time * 1000,
                    request_time * 1000)))

        if cfg['header_stats']:
            response['X-QueryInspect-Num-SQL-Queries'] = str(n)
            response['X-QueryInspect-Total-SQL-Time'] = '%d ms' % (
                sql_time * 1000)
            response['X-QueryInspect-Total-Request-Time'] = '%d ms' % (
                request_time * 1000)
            response['X-QueryInspect-Duplicate-SQL-Queries'] = str(
                num_duplicates)

    def process_request(self, request):
        if not self.ignore_request(request.path):
            self.request_start = time.time()
            self.conn_queries_len = len(connection.queries)

    def process_response(self, request, response):
        if (
            hasattr(self, "request_start") and
            not self.ignore_request(request.path)
        ):
            request_time = time.time() - self.request_start

            infos = self.get_query_infos(
                connection.queries[self.conn_queries_len:])

            num_duplicates = self.check_duplicates(infos)
            self.check_stddev_limit(infos)
            self.check_absolute_limit(infos)
            self.output_stats(infos, num_duplicates, request_time, response)

        return response


if cfg['enabled'] and cfg['log_tbs']:
    QueryInspectMiddleware.patch_cursor()
