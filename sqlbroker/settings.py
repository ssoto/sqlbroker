#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: settings
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

DEBUG = True

DBACCESS = {
    'mysql': {
        'data': {
            'host': '',
            'port': 3306,
            'db': 'wan',
            'user': '',
            'passwd': '',
            'protocol': 'mysql'
        }
    },
    'druid': {
        'data': {
            'host': 'D-RSC-WAN-DRU05.wtelecom.es',
            'port': 8080,
            'db': '',
            'user': '',
            'passwd': '',
            'protocol': 'http',
            'url_root_path': 'druid/v2/',
            'query_timeout': 60000,  # milisec
            'query_limit': 20  # default limit for results
        }
    },
    'elasticsearch': {
        'data': {
            'host': '',
            'port': 9200,
            'db': '',
            'user': '',
            'passwd': ''
        }
    }
}
