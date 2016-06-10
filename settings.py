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
            'db': '',
            'user': '',
            'passwd': ''
        }
    },
    'druid': {
        'data': {
            'host': '',
            'port': 8080,
            'db': '',
            'user': '',
            'passwd': '',
            'protocol': 'http',
            'url_root_path': 'druid/v2/'
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