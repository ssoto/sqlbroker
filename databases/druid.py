#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: druid
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

import requests
from pydruid.client import PyDruid
from sqlbroker.settings import DBACCESS, DEBUG
from sqlbroker.lib.utils import Singleton, NoExistingQuery


class DruidManager(object):
    """Manager for Druid engine."""
    
    __metaclass__ = Singleton

    def __init__(self):
        super(DruidManager,self).__init__()
        #self.arg = arg
        self.proto = DBACCESS[druid][data][protocol]
        self.host = DBACCESS[druid][data][host]
        self.port = DBACCESS[druid][data][port]
        self.url_root_path = DBACCESS[druid][data][url_root_path]

        self.urlconn = self.proto + '://' + self.host + ':' port

        # For console debugging:
        if DEBUG == True:
            print 'Druid instance created.'


    def sql_to_pydruid(self, dicc):
        """ SQL to JSON converter. """

        # ---------------------- TODO ----------------------
        datasource = dicc['FROM']
        
        intervals

        aggregations = dict()

        
        dimensions = dicc['GROUP BY']
        filter 
        metric
        
        post_aggregations
        threshold
        granularity

        return (datasource, granularity, intervals, aggregations,
            post_aggregations, dimensions, filter, metric, threshold)


        #-----------------------------------------------------
        
    def query(self, qtype, statement):
        """ Two types of queries: JSON (native format) or SQL.
            SQL statement looks like a dictionary where the keys
            are the SQL clause keywords and values are the
            companions identifiers of SQL keywords.
        """

        if qtype == 'json':
            # Native query for tests using 'requests' module
            # ... TODO ...

            # result = ...

        elif qtype == 'sql':
            # SQL query will be traduced to paramenters PyDruid understands.
            #json_query = sql_to_json(statement)

            

            # map SQL clauses to Druid query parameters:
            # ... TODO ...

            query = PyDruid(self.urlconn, self.url_root_path)

            # length of dimension list = type of query
            #
            #   - timeseries:           long (dimension) = 0
            #   - topN:                 long (dimension) = 1
            #   - groupby (nested topN):long (dimensiones) = 1..N
            sql_to_pydruid(statement)

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