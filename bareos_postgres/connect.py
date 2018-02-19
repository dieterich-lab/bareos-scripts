#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__      = "Mike Rightmire"
__copyright__   = "Universitäts Klinikum Heidelberg, Section of Bioinformatics and Systems Cardiology"
__license__     = "Not licensed for private use."
__version__     = "0.9.0.0"
__maintainer__  = "Mike Rightmire"
__email__       = "Michael.Rightmire@uni-heidelberg.de"
__status__      = "Development"


from argparse       import ArgumentParser
# ConfigHandler disabled until py3 update
# from common.confighandler  import ConfigHandler 
# loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
# from common.loghandler     import log 
from common.checks         import Checks
checks = Checks() 
_delim = checks.directory_deliminator()
obfpwd = checks.obfuscate_key # Setting as an method object
from common.loghandler import log
from inspect import stack
from bareos_postgres.ABC import Bareos_postgres_ABC

import inspect
import ntpath
import os
import sqlalchemy
        

class Connect(Bareos_postgres_ABC):
    """"""
    def __init__(self, parser = {}, *args, **kwargs):
        # Always set the defaults via the @property
        if isinstance(parser, ArgumentParser):
            parser.add_argument('--user',        '-U', action="store", dest="user",       type=str, default = None, help="Database User for accessing the Bareos Postgres database. (DEFAULT: 'bareospostgresro')")
            parser.add_argument('--password',    '-P', action="store", dest="password",   type=str, default = None, help="Database User's Password for accessing the Bareos Postgres database. (DEFAULT: 'None')")
            parser.add_argument('--host',        '-P', action="store", dest="host",       type=str, default = None, help="Host for accessing the Bareos Postgres database. (DEFAULT: 'localhost')")
            parser.add_argument('--port',        '-p', action="store", dest="port",       type=str, default = None, help="Port for accessing the Bareos Postgres database. (DEFAULT: '5432')")
            parser.add_argument('--database',    '-P', action="store", dest="database",   type=str, default = None, help="Bareos Postgres database name to which to connect. (DEFAULT: 'bareos')")
            
        super().__init__(parser, args, kwargs)

        # Always set the defaults via the @property
        self.user       = kwargs.get("user",      None)
        self.password   = kwargs.get("password" , None)
        self.host       = kwargs.get("host" ,     None)
        self.port       = kwargs.get("port" ,     None)
        self.database   = kwargs.get("database" , None)
        
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(self.user, self.password, self.host, self.port, self.database)
        # The return value of create_engine() is our connection object
        try:
            self.engine = sqlalchemy.create_engine(url, client_encoding='utf8')

        except Exception as e:
            err = "Unable to create Postgres engine with  user = '{U}', password = '{P}',  host= '{H}', port = '{O}', database = '{D}'".format( U = self.user, 
                                                                                                                                                P = obfpwd(self.password), 
                                                                                                                                                H = self.host,
                                                                                                                                                O = self.port,
                                                                                                                                                D = self.database
                                                                                                                                                )
            log.error(err)
            raise RuntimeError(err)

        try:
            self.meta =sqlalchemy.MetaData(bind=self.engine, reflect=True)
        
        except Exception as e:
            err = "Unable to create Postgres Meta object from SQLAlchemy engine '{E}'".format( E = str(self.engine))
            log.error(err)
            raise RuntimeError(err)
            
    @property
    def user(self):
        try:
            return self.USER
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @user.setter
    def user(self, value):
        if value is None: value = "bareospostgresro"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.USER = _value
    
    @user.deleter
    def user(self):
        del self.USER        
        
    @property
    def password(self):
        try:
            return self.PWD
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @password.setter
    def password(self, value):
        if value is None: value = ""
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.PWD = _value
    
    @password.deleter
    def password(self):
        del self.PWD

    @property
    def host(self):
        try:
            return self.HOST
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @host.setter
    def host(self, value):
        if value is None: value = "localhost"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.HOST = _value
    
    @host.deleter
    def host(self):
        del self.HOST

    @property
    def port(self):
        try:
            return self.PORT
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @port.setter
    def port(self, value):
        if value is None: value = "5432"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.PORT = _value
    
    @port.deleter
    def port(self):
        del self.PORT

    @property
    def database(self):
        try:
            return self.DATABASE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @database.setter
    def database(self, value):
        if value is None: value = "bareos"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.DATABASE = _value
    
    @database.deleter
    def database(self):
        del self.DATABASE

    @property        
    def engine(self):
        try: return self.ENGINE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. Try the 'connection' method. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
                
    @engine.setter
    def engine(self, value):
        if isinstance(value, sqlalchemy.engine.base.Engine):
            self.ENGINE = value

        else:
                err = "Value '{V}'does not appear to be a valid 'sqlalchemy.engine.base.Engine' object".format(V = type(value)) 
                log.error(err)
                raise ValueError(err)

    @engine.deleter
    def engine(self):
        del self.ENGINE
            
    @property        
    def meta(self):
        try: return self.META
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. Try the 'connection' method. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)                        

    @meta.setter
    def meta(self, value):
        if isinstance(value, sqlalchemy.sql.schema.MetaData):
            self.META = value
        else:
                err = "Value '{V}'does not appear to be a valid 'sqlalchemy.sql.schema.MetaData' object".format(V = type(value)) 
                log.error(err)
                raise ValueError(err)

    @meta.deleter
    def meta(self):
        del self.META

        
    
#===============================================================================
# if __name__ == '__main__':
#     parser = ArgumentParser()
#     object = ClassName(parser)
#===============================================================================

    def execute(self, *args, **kwargs):
        print(self.ENGINE)
        print("args:",args)
        print("kwargs:", kwargs)
        self.ENGINE(args, kwargs)