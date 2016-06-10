#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: mysql
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

from dbmanager.lib.utils import Singleton


class MysqlManager(object):
	"""Manager for MySQL engine."""
	
	__metaclass__ = Singleton

	def __init__(self):
		super(MysqlManager,self).__init__()
		#self.arg = arg
		print 'MySQL instance created.'

	def print_arg(self, value):
		""" Test """
		return value
