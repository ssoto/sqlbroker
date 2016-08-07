# sqlbroker
Universal broker to process and forward SQL queries to several datasource backends, including relational and NO-SQL DDBBs

# Getting started
To start you must:
```
cd sqlbroker
virtualenv env
source env/bin/activate
pip install -r requirements.info
python setup.py develop
```

With this commands, sqlbroker has been installed as package. You can make changes in sqlbroker module and those are updated in your running server.

Run server with:
```
python sqlbroker/broker.py

```
