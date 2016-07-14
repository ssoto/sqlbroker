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
import requests

from pydruid.client import PyDruid
from pydruid.utils.aggregators import *
from pydruid.utils import filters
from pydruid.utils.filters import *

from sqlbroker.settings import DBACCESS, DEBUG
from sqlbroker.lib.utils import Singleton, NoExistingQuery
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
        params['datasource'] = dicc['FROM'] if dicc['FROM'] else ''

        # Define query interval and granularity:
        params['intervals'] = dicc['QINTERVAL'] if dicc['QINTERVAL'] else ''
        params['granularity'] = dicc['GRANULARITY'] if dicc['GRANULARITY'] \
            else ''

        # Parse GROUPBY chain: grouping
        params['dimensions'] = dicc['GROUP BY'] if dicc['GROUP BY'] else ''
        params['metric'] = re.sub(r'\s(DE|A)SC','', dicc['ORDER BY']) if \
            dicc['ORDER BY'] else ''

        # Parse LIMIT chain
        params['threshold'] = dicc['LIMIT'] if dicc['LIMIT'] else ''

        # # Parse WHERE chain
        if dicc['WHERE']:
            # pattern = re.compile(r'(?P<id>[\w.]+)\s?(?P<op>==)\s?(?P<value>[\w.-]+|[\'\"].*?[\'\"])')
            
            # clauses_ = pattern.sub(filter_dimension, dicc['WHERE'])
            # print '###', clauses_

            clauses_ = re.sub(
                r'(?P<id>[\w.]+)\s?(?P<op>[<>]=?)\s?(?P<value>[\w.-]+|[\'\"].*?[\'\"])',
                '(getattr(filters,\'JavaScript\')(\'\g<id>\') = \"function(v) { return v \g<op> \g<value> }\")',
                dicc['WHERE'],
                re.M|re.S)

            clauses = re.sub(
                r'(?P<id>[\w.]+)\s?(?P<op>\!=|=)\s?(?P<value>[\w.-]+|[\'\"].*?[\'\"])',
                '(getattr(filters,\'Dimension\')(\'\g<id>\') \g<op> \g<value>)',
                clauses_,
                re.M|re.S)

            conditions_ = re.sub(r'[^<>!]=', ' ==', clauses, re.M|re.S)
            conditions_ = re.sub(r'AND', '&', conditions_, re.M|re.S)

            conditions = re.sub(r'OR', '|', conditions_, re.M|re.S)
            print conditions
            params['filter'] = conditions

        else:
            params['filter'] = ''

        #TODO: parse SELECT (aggregations), HAVING clause (grouping conditions)

        # Parse SELECT aggs and match with GROUP BY
        #aggs_keys = ('SUM', 'AVG', 'MIN', 'MAX', 'COUNT')

        clauses = re.sub(
                r'(?P<op>SUM|AVG|MIN|MAX|COUNT)\s?\(\s?(?P<value>[\w.-]+)\s?\)\s?(?P<alias>AS)\s?(?(alias)[\w.-]+)',
                'agg[\g<4>] = \g<op>(\g<value>)',
                dicc['SELECT'],
                re.M|re.S)

        print 'SELECT', clauses
        
        params['aggregations'] = dict()
        params['aggregations']['Totalinbytes'] = doublesum('inbytes')

        params['post_aggregations'] = dict()

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
            params = self.sql_to_pydruid(dicc)

            # length of dimension list = type of query
            #
            #   - timeseries:           long (dimension) = 0
            #   - topN:                 long (dimension) = 1
            #   - groupby (nested topN):long (dimensiones) = 1..N

            num_dim = list(params['dimensions'].split(',')).__len__()
            
            # Current time on UNIX timestamp format
            ts = time.time().__str__()

            if num_dim == 0:
                query_id = "tseries-" + ts.__str__()
                print "Query-ID:", query_id

                result = self._query[type_](
                    datasource=params['datasource'],
                    granularity=params['granularity'],
                    intervals=params['intervals'],
                    aggregations=params['aggregations'],
                    post_aggregations=params['post_aggregations'],
                    filter=params['filter'],
                    context={"timeout": 60000, "queryId": query_id}
                )

                result = result.result_json

            elif num_dim == 1:
                query_id = "topn-" + ts.__str__()
                print "Query-ID:", query_id

                result = self._query.topn(
                    datasource=params['datasource'],
                    granularity=params['granularity'],
                    intervals=params['intervals'],
                    aggregations=params['aggregations'],
                    post_aggregations=params['post_aggregations'],
                    filter=eval(params['filter']),
                    dimension=params['dimensions'],
                    metric=params['metric'],
                    threshold=params['threshold'],
                    context={"timeout": 60000, "queryId": query_id}
                )

                result = result.result_json

            else:
                query_id = "gby-" + ts.__str__()
                print "Query-ID:", query_id

                result = '{ "state": "toDo"}'                

        else:
            raise NoExistingQuery

        return result



#  ts = query.timeseries(
# #     datasource='twitterstream',
# #     granularity='day',
# #     intervals='2014-02-02/p4w',
# #     aggregations={'length': doublesum('tweet_length'), 'count': doublesum('count')},
# #     post_aggregations={'avg_tweet_length': (Field('length') / Field('count'))},
# #     filter=Dimension('first_hashtag') == 'sochi2014'
# # )

#  top = query.topn(
# #     datasource='twitterstream',
# #     granularity='all',
# #     intervals='2014-03-03/p1d',  # utc time of 2014 oscars
# #     aggregations={'count': doublesum('count')},
# >>     dimension='user_mention_name',
     # filter=(Dimension('user_lang') == 'en') & (Dimension('first_hashtag') == 'oscars') &
     #        (Dimension('user_time_zone') == 'Pacific Time (US & Canada)') &
     #        ~(Dimension('user_mention_name') == 'No Mention'),
# >>     metric='count',
# >>     threshold=10
# # )

#  group = query.groupby(
# #     datasource='twitterstream',
# #     granularity='hour',
# #     intervals='2013-10-04/pt12h',
# >>     dimensions=["user_name", "reply_to_name"],
# #     filter=(~(Dimension("reply_to_name") == "Not A Reply")) &
# #            (Dimension("user_location") == "California"),
# #     aggregations={"count": doublesum("count")}
# # )