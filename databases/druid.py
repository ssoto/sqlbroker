#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: druid
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

import re
import time
from math import ceil
#import requests

from pydruid.client import PyDruid
from pydruid.utils import filters
from pydruid.utils.filters import *
from pydruid.utils import aggregators
from pydruid.utils.aggregators import *
from pydruid.utils import postaggregator
from pydruid.utils.postaggregator import *


from sqlbroker.settings import DBACCESS, DEBUG
from sqlbroker.lib.utils import Singleton, QueryError
from sqlbroker.lib.sqlparser import SQLParser as SQLP


class DruidManager(object):
    """Manager for Druid engine."""
    
    __metaclass__ = Singleton

    def __init__(self):

        super(DruidManager,self).__init__()

        self._proto = DBACCESS['druid']['data']['protocol']
        self._host = DBACCESS['druid']['data']['host']
        self._port = DBACCESS['druid']['data']['port']
        self._url_root_path = DBACCESS['druid']['data']['url_root_path']
        self._qtimeout = DBACCESS['druid']['data']['query_timeout']
        self._qlimit = DBACCESS['druid']['data']['query_limit']

        self._granularities = {'second': 'PT1S', 'minute': 'PT1M',
            'fifteen_minute': 'PT15M', 'thirty_minute': 'PT30M', 'hour': 'PT1H',
            'day': 'P1D', 'week': 'P1W', 'month': 'P1M', 'quarter': 'P3M',
            'year': 'P1Y'}

        self._urlconn = self._proto + '://' + self._host + ':' + \
            self._port.__str__()

        # Druid connection:
        self._query = PyDruid(self._urlconn, self._url_root_path)

        # For console debugging:
        if DEBUG == True:
            print 'Druid instance created.'


    def sql_to_pydruid(self, dicc):
        """ SQL to JSON converter. """

        #-----------------------------------------------------
        # TODO: replace fixed params by SQL clause values

        params = dict()

        # Datasource:
        if 'FROM' in dicc:
            params['datasource'] = dicc['FROM']
        else:
            raise QueryError('Undefined datasource (from)')

        # Define query interval and granularity:
        if 'QINTERVAL' in dicc:
            params['intervals'] = dicc['QINTERVAL']
        else:
            raise QueryError('Undefined query time interval (qinterval)')

        if 'GRANULARITY' in dicc:
            params['granularity'] = dicc['GRANULARITY'].lower()
        else:
            raise QueryError('Undefined time granularity (granularity)')

        # Parse GROUP_BY chain: grouping
        # >> If no GROUP_BY clause is defined, query will be processed as a
        # >> timeseries query.
        if 'GROUP BY' in dicc:
            params['dimensions'] = dicc['GROUP BY']

        # Parse 'ORDER_BY' clause for TopN queries:
        if 'ORDER BY' in dicc:    
            params['metric'] = re.sub(r'\s(DE|A)SC','', dicc['ORDER BY'])

        # Parse LIMIT clause for TopN queries:
        if 'LIMIT' in dicc:
            params['threshold'] = dicc['LIMIT']
        else:
            params['threshold'] = self._qlimit

        # # Parse WHERE chain (always is a optional clause)
        if 'WHERE' in dicc:

            clauses_ = re.sub(
                r'(?P<id>[\w.]+)\s?(?P<op>[<>]=?)\s?'
                r'(?P<value>[\w.-]+|[\'\"].*?[\'\"])',
                '(getattr(filters,\'JavaScript\')(\'\g<id>\') = '
                '\"function(v) { return v \g<op> \g<value> }\")',
                dicc['WHERE'],
                re.M|re.S)

            clauses_ = re.sub(
                r'(?P<id>[\w.]+)\s?(?P<op>\!=|=)\s?'
                '(?P<value>[\w.-]+|[\'\"].*?[\'\"])',
                '(getattr(filters,\'Dimension\')(\'\g<id>\') \g<op> \g<value>)',
                clauses_,
                re.M|re.S)

            clauses = re.sub(r'[^<>!]=', ' ==', clauses_, re.M|re.S)
            clauses = re.sub(r'AND', '&', clauses, re.M|re.S)

            params['filter'] = re.sub(r'OR', '|', clauses, re.M|re.S)
            
            if DEBUG:
                print 'WHERE: ', params['filter']

        else:
            params['filter'] = ''

        # Parse SELECT aggs and match with GROUP BY
        params['aggregations'] = dict()
        params['post_aggregations'] = dict()

        # Function to use into re.sub, in order to parse (post)aggregation
        # operations and fill in 'aggregations' and 'post_aggregations'
        # dictionaries.
        # Function output is captured by re.sub but is not used.
        def repl_agg(m):
            op = {'SUM': 'doublesum', 'MIN': 'min', 'MAX': 'max',
                'COUNT': 'count'}
            
            post_op = ('AVG', )


            if m.group(4) != None:
                name = 'alias'
            else:
                name = 'value'


            # Standard aggregation operators:
            if m.group('op') in op.keys():

                # rep = '"' + m.group(name) + '": ' + op[m.group('op')] + \
                #     '("' + m.group('value') + '")'
                rep = 'aggregator'

                params['aggregations'][m.group(name)] = \
                    getattr(aggregators, op[m.group('op')])(m.group('value'))


            # Advanced aggregation operators -> postaggregations:
            elif m.group('op') in post_op:

                op1 = 'op1_' + m.group('value')
                op2 = 'op2_' + m.group('value')

                params['aggregations'][op1] = \
                    getattr(aggregators, op['SUM'])(m.group('value'))
                
                params['aggregations'][op2] = \
                    getattr(aggregators, op['COUNT'])(m.group('value'))

                ## AVG operator case:
                if m.group('op') == 'AVG':

                    params['post_aggregations'][m.group(name)] = \
                        getattr(postaggregator, 'Field')(op1) / \
                        getattr(postaggregator, 'Field')(op2)
                
                ## support for another postagg operators..
                rep = 'postaggregator'


            # Unknown aggregation operation:
            else:
                raise QueryError('Unknown aggregation operator.')

            return rep

        clauses = re.sub(
            r'((?P<op>[A-Z]+?)\s*?\(\s*?(?P<value>[\w.-]+)\s*?\))'
            r'(\s*?(?P<as>AS)\s*?(?P<alias>[\w.-]+))?',
            repl_agg, dicc['SELECT'], re.M|re.S)


        if DEBUG:
            print 'Aggregations: ', params['aggregations']
            print 'Postaggregations: ', params['post_aggregations']


        #TODO: HAVING clause (grouping conditions)

        return params

        #-----------------------------------------------------
   

    def query(self, qtype, statement):
        """ Two types of queries: JSON (native format) or SQL.
            SQL statement looks like a dictionary where the keys
            are the SQL clause keywords and values are the
            companions identifiers of SQL keywords.
        """

        if qtype == 'json':
            # Native query for tests using 'requests' module

            # TODO **************

            pass

        elif qtype == 'sql':
            # SQL query will be traduced to paramenters PyDruid understands.
            
            # SQL statements to list of dictionaries with SQL clauses as keys,
            # one query per dicc in dicc-list:
            parser = SQLP()
            dicc = parser.parse(statement)
            
            # map SQL clauses to Druid query parameters:
            try:
                params = self.sql_to_pydruid(dicc)

                # length of dimension list = type of query
                #
                #   - timeseries:           long (dimension) = 0
                #   - topN:                 long (dimension) = 1
                #   - groupby (nested topN):long (dimensiones) = 1..N

                if 'dimensions' in params:
                    num_dim = list(params['dimensions'].split(',')).__len__()
                else:
                    num_dim = 0
                
                # Current time on UNIX timestamp format
                ts = time.time().__str__()


                # -- Timeseries query --
                if num_dim == 0:
                    query_id = "tseries-" + ts.__str__()
                    
                    if DEBUG:
                        print "Query-ID:", query_id

                    result = self._query.timeseries(
                        datasource = params['datasource'],
                        granularity = params['granularity'],
                        intervals = params['intervals'],
                        aggregations = params['aggregations'],
                        post_aggregations = params['post_aggregations'],
                        filter = eval(params['filter']),
                        context = {"timeout": self._qtimeout,
                            "queryId": query_id}
                    )

                    result = result.result_json

                # -- Basic Top-N query --
                elif num_dim == 1:
                    query_id = "topn-" + ts.__str__()

                    if DEBUG:
                        print "Query-ID:", query_id

                    result = self._query.topn(
                        datasource = params['datasource'],
                        granularity = params['granularity'],
                        intervals = params['intervals'],
                        aggregations = params['aggregations'],
                        post_aggregations = params['post_aggregations'],
                        filter = eval(params['filter']),
                        dimension = params['dimensions'],
                        metric = params['metric'],
                        threshold = params['threshold'],
                        context = {"timeout": self._qtimeout,
                            "queryId": query_id}
                    )

                    result = result.result_json

                # -- Nested Top-N query: alternative to expensive groupby --
                #
                # An accuracy error can exists due to the prunning in
                # intermediate results from previous queries.
                # Operation:
                #  0) N = query threshold = value of LIMIT SQL clause
                #  1) Execute Top-N over dimension-1, aggregating over metrics.
                #  2) Execute 'N' x Top-N/2 over dimension-2, filtering by
                #     results from 1), aggregating over metrics again.
                #  3) Repeat 1) and 2) swapping dimension-1 and dimension-2
                #  4) Group results from 2) and 3), and return the greatest N
                #     results based on metrics aggregation.
                #
                
                # elif num_dim in (2, 3):
                elif num_dim == 2:

                    query_id = "nestopn-" + ts.__str__()

                    if DEBUG:
                        print "Query-ID:", query_id

                    th_l1 = params['threshold']
                    th_l2 = ceil(float(th_l1) / 2)

                    dim = list(params['dimensions'].split(','))

                    dim1 = dim[0].strip()
                    dim2 = dim[1].strip()

                    # Initial query: TopN over first dimension
                    res = self._query.topn(
                        datasource = params['datasource'],
                        granularity = params['granularity'],
                        intervals = params['intervals'],
                        aggregations = params['aggregations'],
                        post_aggregations = params['post_aggregations'],
                        filter = eval(params['filter']),
                        dimension = dim1,
                        metric = params['metric'],
                        threshold = th_l1,
                        context = {"timeout": self._qtimeout,
                            "queryId": query_id}
                    )

                    qresult=json.loads(res.result_json)

                    # Make a dictionary where the keys are the intervals
                    # (funtion of query granularity), and the values are the
                    # TopN values of the first dimension, "dim1", for each
                    # interval.
                    dic_dim1=dict()

                    # qresult is a list of dictionaries
                    for qres in qresult:
                        interval = qres['timestamp']
                        list_dim1 = list()

                        for elem in qres['result']:
                            list_dim1.append(elem[dim1])

                        dic_dim1.update({interval : list_dim1})

                    # Dictionary for final result
                    result = list()

                    # Then, launch N queries by interval, one for each value
                    # of the dimension-1 in the interval. The type of the 
                    # queries will be TopN too, but the dimension will be
                    # dimension-2, filtering with dimension-1 every time.
                    for interval in dic_dim1.keys():
                        values_dim1 = dic_dim1[interval]

                        if DEBUG:
                            print '>> Interval: ', interval

                        l2_result = list()

                        for val_dim1 in values_dim1:
                            query_id = "nestopn-" + ts.__str__()

                            # TopN query: dimension = dim2, threshold = th_l2
                            res = self._query.topn(
                                datasource = params['datasource'],
                                granularity = 'all',
                                intervals = interval + '/' + \
                                self._granularities[params['granularity']],
                                aggregations = params['aggregations'],
                                post_aggregations = params['post_aggregations'],
                                filter = (Dimension(dim1) == val_dim1) &
                                    (eval(params['filter'])),
                                dimension = dim2,
                                metric = params['metric'],
                                threshold = th_l2,
                                context = {"timeout": self._qtimeout,
                                    "queryId": query_id}
                            )

                            if DEBUG:
                                print 'Dim1: ', val_dim1
                                print res.result_json

                            qresult=json.loads(res.result_json)

                            for qres in qresult:
                                for elem in qres['result']:
                                    elem_merged = elem.copy()
                                    elem_merged.update({dim1: val_dim1})

                                    l2_result.append(elem_merged)

                        result.append({"timestamp": interval, "result": l2_result})
                        result = json.dumps(result)


                # -- GroupBy query (no limit over results) --
                else:
                    query_id = "gby-" + ts.__str__()

                    if DEBUG:
                        print "Query-ID:", query_id

                    
                    result = self._query.groupby(
                        datasource = params['datasource'],
                        granularity = params['granularity'],
                        intervals = params['intervals'],
                        aggregations = params['aggregations'],
                        post_aggregations = params['post_aggregations'],
                        filter = eval(params['filter']),
                        dimensions = params['dimensions'],
                        context = {"timeout": self._qtimeout,
                            "queryId": query_id}
                    )

                    result = result.result_json


            except Exception as err:
                # Re-launch exception to manage in main program:
                
                if DEBUG:
                    import traceback
                    traceback.print_exc()

                # Launch last exception:
                raise
    
        else:
            #result = 'Query type no recognized.'
            pass

        return result



# ----------------------------------------------------------------------------
# Pydruid query examples:
# ----------------------------------------------------------------------------

#  ts = query.timeseries(
#      datasource='twitterstream',
#      granularity='day',
#      intervals='2014-02-02/p4w',
#      aggregations={'length': doublesum('tweet_length'), 'count': doublesum('count')},
#      post_aggregations={'avg_tweet_length': (Field('length') / Field('count'))},
#      filter=Dimension('first_hashtag') == 'sochi2014'
#  )

#  top = query.topn(
#      datasource='twitterstream',
#      granularity='all',
#      intervals='2014-03-03/p1d',  # utc time of 2014 oscars
#      aggregations={'count': doublesum('count')},
#*#     dimension='user_mention_name',
#      filter=(Dimension('user_lang') == 'en') & (Dimension('first_hashtag') == 'oscars') &
#      (Dimension('user_time_zone') == 'Pacific Time (US & Canada)') &
#      ~(Dimension('user_mention_name') == 'No Mention'),
#*#      metric='count',
#*#      threshold=10
#  )

#  group = query.groupby(
#      datasource='twitterstream',
#      granularity='hour',
#      intervals='2013-10-04/pt12h',
#*#     dimensions=["user_name", "reply_to_name"],
#     filter=(~(Dimension("reply_to_name") == "Not A Reply")) &
#             (Dimension("user_location") == "California"),
#      aggregations={"count": doublesum("count")}
#  )