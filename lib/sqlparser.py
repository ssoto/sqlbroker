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


    #  'parse' method returns one dict by SQL statement.
    def parse(self, sqlstatements):

        keywords = ('SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING',
            'ORDER BY', 'DISTINCT', 'INSERT INTO', 'UPDATE', 'DELETE FROM')

        operators = ('AND', 'OR')
        othersop = ('BETWEEN', 'ON', 'IN', 'LIKE')

        statements = sqlparse.split(sqlstatements)

        dicclist = list()

        #Split into SQL statements, if there is more than one 
        for st in statements:
            
            sql = sqlparse.format(st, strip_comments=True,
                reindent=True, indent_width='2', keyword_case='upper',
                identifier_case='lower', wrap_after=100000).split('\n')

            dicc = dict()
            flag_where = False

            for line in sql:
                clause = re.match(r'^(\s*[A-Z]+\s?[A-Z]*) (.*)$', line,
                    re.M|re.S )

                if clause:
                        key = clause.group(1).__str__()
                        value = clause.group(2).__str__()
                        print key, value

                        if key in keywords:
                            if key == 'WHERE':
                                flag_where = True
                            else:
                                flag_where = False
                            dicc[key] = value

                        # 'AND' / 'OR' senteneces
                        elif key.strip() in operators and flag_where:
                            dicc['WHERE'] = dicc['WHERE'] + ' ' + key.strip() \
                                + ' '  + value
                            
            dicclist.append(dicc)

        return dicclist


if __name__ == '__main__':

    sqlst = """select GRANULARITY(day), pepe AS pepito, pp, ddd
            from pppp JOIN ppp2 on pppp.id1 = ppp2.id2
            where pppp.id1 BETWEEN 5 AND 7 and PPPP.id1 like \'%dd%\'
            and ppp2.id2 >= 3 GROUP by pepe, pp, ddd HAVING s >= 3 ORDER BY
            pepe DESC;
            select aaa from BBBB;"""

    lista = SQLParser()

    for dicc in lista.parse(sqlst):
        #if 'WHERE' in dicc:
        #   print dicc['WHERE']
        print dicc