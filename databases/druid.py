#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: druid
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

import re
import requests

from pydruid.client import PyDruid
from pydruid.utils.aggregators import *

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

        params['datasource'] = dicc['FROM']
        params['intervals'] = dicc['QINTERVAL']

        params['aggregations'] = dict()
        params['aggregations']['Totalinbytes'] = doublesum('inbytes')

        
        params['dimensions'] = dicc['GROUP BY']
        params['filter'] = ''
        #params['metric'] = 'Totalinbytes'
        params['metric'] = re.sub(r'\s(DE|A)SC','', dicc['ORDER BY'])
        print params['metric']
        params['post_aggregations'] = dict()

        params['threshold'] = dicc['LIMIT']
        params['granularity'] = dicc['GRANULARITY']

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

            num_dim = list(dicc['GROUP BY'].split(',')).__len__()

            if num_dim == 0:
                type_ = 'timeseries'
                
                result = self._query[type_](
                    datasource=params['datasource'],
                    granularity=params['granularity'],
                    intervals=params['intervals'],
                    aggregations=params['aggregations'],
                    post_aggregations=params['post_aggregations'],
                    filter=params['filter']
                )

            elif num_dim == 1:
                result = self._query.topn(
                    datasource=params['datasource'],
                    granularity=params['granularity'],
                    intervals=params['intervals'],
                    aggregations=params['aggregations'],
                    post_aggregations=params['post_aggregations'],
                    #filter=params['filter'],
                    dimension=params['dimensions'],
                    metric=params['metric'],
                    threshold=params['threshold']
                )

            else:
                type_ = 'groupby'
                result = '{ "state": "toDo"}'                

        else:
            raise NoExistingQuery



        return result.result_json



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
# #     filter=(Dimension('user_lang') == 'en') & (Dimension('first_hashtag') == 'oscars') &
# #            (Dimension('user_time_zone') == 'Pacific Time (US & Canada)') &
# #            ~(Dimension('user_mention_name') == 'No Mention'),
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