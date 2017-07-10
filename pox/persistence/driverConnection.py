import logging
import psycopg2
import mysql.connector
from mysql.connector import errorcode, Error

class DriverConnection(object):
    '''
    classdocs
    '''
    _db_connection = None
    _db_cursor = None

    def __init__(self):
      '''
      Constructor
      '''
      self.connect()
      #print("dbconnected")


    def connect(self):
      try:
        #self._db_connection = mysql.connector.connect(user='rooty', password='qwertz', host='192.168.182.1', database='controllerdb')
        self._db_connection = psycopg2.connect("dbname='controllerdb' user='postgres' host='192.168.182.1' password='qwertz'")
      except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          logging.info("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          logging.info("Database does not exist")
        else:
          print(err)

    def getCursor(self):
      return self._db_connection.cursor()

    def commit(self):
      self._db_connection.commit()

    def close(self):
      self._db_connection.close()

