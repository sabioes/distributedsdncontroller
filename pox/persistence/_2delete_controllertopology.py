'''
Created on 04/04/2017

@author: Carlos Silva
'''

# from pox.core import core
import logging
import mysql.connector
from mysql.connector import errorcode, Error

class ExternalStore(object):
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
        self._db_connection = mysql.connector.connect(user='root', password='qwertz', host='127.0.0.1', database='controllerdb')

      except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          logging.info("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          logging.info("Database does not exist")
        else:
          print(err)

    def close(self):
      self._db_connection.close()

    def test_query(self):
      query_select = "SELECT * FROM objectkeyvalue"
      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(query_select)
      result = self._db_cursor.fetchall()
      self._db_cursor.close()
      return result

    #method tests --TODELETE ALL METHOD
    def tests_insert(self):
      insertObject = ("INSERT INTO objectkeyvalue (objkey, objvalue) VALUES (%s, %s)")
      dataObject = ('object_example','Abstract_data_undefined_data_any_data')

      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      self._db_connection.commit()


      print(last_objectid)

      self._db_cursor.close()

    def commit(self, key, value):
      insert = ("INSERT INTO objectkeyvalue(objkey, objvalue) VALUES (%s, %s)")
      data = (key, value)

      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(insert, data)
      idlast = self._db_cursor.lastrowid

      self._db_connection.commit()

      print("last id insert on table objectkeyvalue is: "+ idlast)
      self._db_connection.close()