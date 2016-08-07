#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: mysql
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

from sqlbroker.lib.utils import Singleton


class MysqlManager(object):
    """Manager for MySQL engine."""
    
    __metaclass__ = Singleton



    def __init__(self):

        super(MysqlManager,self).__init__()

        self._proto = DBACCESS['mysql']['data']['protocol']
        self._host = DBACCESS['mysql']['data']['host']
        self._port = DBACCESS['mysql']['data']['port']
        self._db = DBACCESS['mysql']['data']['db']

        #self._urlconn = self._proto + '://' + self._host + ':' + \
        #    self._port.__str__() + '/' + self._db

        # MySQL connection:
        #self._query = ...(self.urlconn)

        # For console debugging:
        if DEBUG == True:
            print 'MySQL instance created.'

    #-----------------------------------------------------
    
    def print_arg(self, value):
        """ Test """
        return value

    #-----------------------------------------------------
    
    def query(self, qtype, statement):
        """ Two types of queries: JSON (native format) or SQL.
            SQL statement looks like a list of SQL sentences.
        """

        if qtype == 'sql':

            # TODO ***************    
            print statement

        else:
            raise NoExistingQuery


        return result
