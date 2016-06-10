#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: broker
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

import os
import json

from flask import Flask, request, url_for

from sqlbroker.settings import DEBUG
from sqlbroker.lib.sqlparser import SQLParser as SQLP
from sqlbroker.lib.utils import DynamicImporter


app = Flask(__name__)

@app.route('/')
def index():
    return 'running'


@app.route('/dbmanager/<ddbb>', methods=['GET', 'POST'])
def discriminator(ddbb):

    manager_root_path = 'databases'
    manager_module = ddbb
    manager_class = ddbb + '_manager'
    
    dimporter = DynamicImporter(manager_root_path, manager_module, 
        manager_class)
    
    dbmanager = dimporter.instance_class()

    result = ''

    #JSON statement (native format)
    if request.method == 'POST' and request.is_json:

        # send request to ddbb directly (Druid expects query in json format)
        result = dbmanager.query('json', request.get_json())
        
    #SQL statement
    elif request.method == 'POST':
        
        parser = SQLP()
        for dicc in parser.parse(request.data):
            
            # One query per dicc in dicc-list:

            result += dbmanager.query('sql', dicc)
    
    return result


with app.test_request_context():
    print '********************'
    print 'Available URLs:'
    print url_for('index')
    print url_for('discriminator', ddbb='druid')
    print '********************'


if __name__ == "__main__":
    app.run(host='10.200.3.35', port='3000', threaded=True)

