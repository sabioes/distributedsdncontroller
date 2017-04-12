'''
Created on 04/04/2017

@author: Carlos Silva
'''
# coding: latin-1
import mysql.connector
from mysql.connector import errorcode

class Db_connector(object):
    '''
    classdocs
    '''
    _db_connection = None
    _db_cursor = None
    
    def __init__(self):
        '''
        Constructor
        '''
      
    def connect(self):
      try:
        self._db_connection = mysql.connector.connect(user='root', password='qwerty', host='127.0.0.1', database='pox_dstl2')
        self._db_cursor = self._db_connection.cursor()
      except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          print("Database does not exist")
        else:
          print(err)
      #else:
        #self._db_connection.close()
  
    def query(self, query, params):
      self._db_cursor.execute(query, params)
      
    def query_simples(self):
      query_simples = "SELECT * FROM objectDATA"
      
      self._db_cursor.execute(query_simples)
      
      for (id_data, plugin_name, object_name, object_content) in self._db_cursor:
        print("id {}, has {} | {} | {}".format(id_data, plugin_name, object_name, object_content))
        
      self._db_cursor.close()
      
    def insert(self, params=None):
      insertObject = ("INSERT INTO objectdata (plugin_name, object_name, object_content) VALUES (%s, %s, %s)")
      dataObject = ('plug_example','object_example','Abstract_data_undefined_data_any_data')
      
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      
      self._db_connection.commit()
      
      #test
      print(last_objectid)
      
      self._db_cursor.close()
      
    def close(self):
      self._db_connection.close()
      
    
class ExternalStore(object):
    '''
    classdocs
    ''' 
    _db_database = None 
    
    def __init__(self):
        '''
        Constructor
        '''
        _db_database = Db_Connector()
      
    def sendObject(self, parameter_object=None):
      
      print("Sending object")
      print(object)