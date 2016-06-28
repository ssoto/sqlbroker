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

        keywords = ('SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING',
            'ORDER BY', 'DISTINCT', 'INSERT INTO', 'UPDATE', 'DELETE FROM')

        operators = ('AND', 'OR')
        othersop = ('BETWEEN', 'ON', 'IN', 'LIKE')

        sql = sqlparse.format(sqlstatement, strip_comments=True,
            reindent=True, indent_width=2, keyword_case='upper',
            identifier_case='lower', wrap_after=100000).split('\n')

        dicc = dict()
        flag = False

        for line in sql:

            clause = re.match(r'^(\s*[A-Z]*\s?[A-Z]*) (.*)$', line,
                re.M|re.S )

            key = clause.group(1).__str__().strip()
            value = clause.group(2).__str__()

            # key contained in keywords:
            if key in keywords:
                dicc[key] = value
                keybefore = key

            # value that belongs to the previous key (empty key):
            elif key == '':
                dicc[keybefore] = dicc[keybefore] + ' ' + value

            # 'AND', 'OR' sentences that belong to previous key too:
            elif key in operators:
                dicc[keybefore] = dicc[keybefore] + ' ' + key + ' '  + \
                    value

        return dicc


if __name__ == '__main__':

    sqlst = """select GRANULARITY(day), pepe AS pepito, pp, ddd
            from pppp JOIN ppp2 on pppp.id1 = ppp2.id2
            where pppp.id1 BETWEEN 5 AND 7 and PPPP.id1 like '%dd%'
            and ppp2.id2 >= 3 GROUP by pepe, pp, ddd HAVING s >= 3 ORDER BY
            pepe DESC"""

    statement = SQLParser()

    print statement.parse(sqlst)
