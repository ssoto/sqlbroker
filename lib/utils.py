#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: utils
   :platform: Unix, Windows, Mac
   :synopsis: DDBB Manager.
.. moduleauthor:: Ignacio Campos Rivera <icamposrivera@gmail.com>
"""

import os


class Singleton(type):
    
    _instances = {}

    def __call__(cls, *args, **kwargs):
    
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args,
                **kwargs)

        #If you want to run __init__ every time the class is called, add
        #else:
        #    cls._instances[cls].__init__(*args, **kwargs)

        return cls._instances[cls]


class DynamicImporter:
    """Module and class importer from dynamic names"""
 
    def __init__(self, module_root_path, module_name, class_name):
        """Constructor"""
        self._path = module_root_path
        self._module = module_name
        self._class = self.underscore_to_camelcase(class_name)


    def instance_class(self):
        """Instance dynamic module.class"""
        try:
            if os.path.isfile(self._path + '/' + self._module + '.py'):
                
                module_ = __import__(self._path + '.' + self._module,
                    fromlist=[self._class])

                class_ = getattr(module_, self._class)
                
                instance = class_()

                return instance

        except NoDDBB:
            pass
        
        return False
        

    def underscore_to_camelcase(self, value):
        """Convert underscored string to camelcased string"""
        
        name = ''
        for elem in value.split('_'):
            subname = elem.lower()
            name += subname.capitalize()

        return name


class NoDDBB(Exception):
    """No existing DDBB exception."""

    def __init__(self, arg):
        super(Exception, self).__init__()
        self.arg = arg

    def __str__(self):
        return repr(self.arg)


class QueryError(Exception):
    """Malformed Query exception."""

    def __init__(self, arg):
        super(Exception, self).__init__()
        self.arg = arg

    def __str__(self):
        return repr(self.arg)
