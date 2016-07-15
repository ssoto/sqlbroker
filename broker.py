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

from sqlbroker.settings import DEBUG, DBACCESS
from sqlbroker.lib.utils import DynamicImporter, QueryError


app = Flask(__name__)

@app.route('/')
def index():
    return 'running'


@app.route('/dbmanager/<ddbb>', methods=['GET', 'POST'])
def launch_query(ddbb):

    manager_root_path = 'databases'
    manager_module = ddbb
    manager_class = ddbb + '_manager'
    
    dimporter = DynamicImporter(manager_root_path, manager_module, 
        manager_class)
    
    dbmanager = dimporter.instance_class()

    result = ''

    #JSON statement (native format):
    if request.method == 'POST' and request.is_json:

        # send request to ddbb directly (Druid expects query in json format)
        try:
            data = dbmanager.query('json', request.get_json())
            status = 'ok'
            res = {'status': status, 'data': data}
            result = res.__str__()

        except Exception as err:
            status = 'error'
            result = {'status': status, 'error': err}
            
    #In another case, request body is supposed to be a SQL statement:
    elif request.method == 'POST':

    # TODO: separar sentencias SQL en sentencias simples y hacer varias llamadas:
        for statement in request.data.__str__().split(';'):
            st = statement.strip()

            if st != '':
                try:
                    data = dbmanager.query('sql', st).__str__()
                    status = "ok"
                    res = {"status": status, "data": data}

                    # TODO: parse 'res'
                    result += res.__str__()

                except Exception as err:
                    status = "error"
                    result = json.dumps({"status": status, "description": err.__str__()})
                    break

    return result


with app.test_request_context():
    print '---------------------------------'
    print 'Available URLs:'
    print url_for('index')
    print url_for('launch_query', ddbb='ddbb')
    print '---------------------------------'


if __name__ == "__main__":
    app.run(host='10.200.3.35', port='3000', threaded=True)

