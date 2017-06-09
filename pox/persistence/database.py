import select
from mysql.connector import errorcode, Error
from pox.persistence.driverConnection import DriverConnection
from pox.openflow.topology import *

class DatabaseInitiator():
  _driverConnection = None
  def __init__(self):
    '''
    Constructor
    '''
    self._driverConnection = DriverConnection()

  def createDatabase(self):
    sql ='''CREATE DATABASE IF NOT EXISTS controllerdb;'''
    
  def createTables(self):
