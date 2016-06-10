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


    # def sql_to_json(self, dicc):
    #   """ SQL to JSON converter. """

    #   json_query = ''

    #   return json_query


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

            query = PyDruid(self.urlconn, url_root_path)

            # result = ...

        else:
            raise NoExistingQuery



        return result

