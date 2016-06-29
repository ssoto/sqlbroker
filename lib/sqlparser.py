#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: sqlparser
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

import re
import sqlparse
from sqlbroker.settings import DEBUG
from sqlbroker.lib.utils import Singleton


class SQLParser(object):
    """Parser to map a SQL statements group in a list of dictionaries."""
        
    __metaclass__ = Singleton


    def __init__(self):
        super(SQLParser,self).__init__()
        
        # For console debugging:
        if DEBUG == True:
            print 'SQLParser instance created.'


    #  'parse' method returns one dict whose keys are SQL clauses.
    def parse(self, sqlstatement):

        sql = sqlparse.format(sqlstatement, strip_comments=True,
            reindent=True, indent_width=2, keyword_case='upper',
            #identifier_case='lower', wrap_after=100000).split('\n')
            wrap_after=100000).split('\n')

        dicc = dict()
        flag = False

        for line in sql:
        
            clause = re.match(r'^(\s*[A-Z]*\s?[A-Z]*)\s(.*)$', line, re.M|re.S)

            dicc[clause.group(1).__str__().strip()] = \
                clause.group(2).__str__().strip()

        return dicc


if __name__ == '__main__':

    sqlst = """select pepe AS pepito, pp, ddd GRANULARITY 3600
            qinterval '2015-08-01/2015-08-02'
            from pppp JOIN ppp2 on pppp.id1 = ppp2.id2
            where pppp.id1 BETWEEN 5 AND 7 and PPPP.id1 like '%dd%'
            and ppp2.id2 >= 3 GROUP by pepe, pp, ddd HAVING s >= 3 ORDER BY
            pepe DESC limit 10"""

    statement = SQLParser()

    print statement.parse(sqlst)
