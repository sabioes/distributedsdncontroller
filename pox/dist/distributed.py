'''
Created on 04/04/2017

@author: Carlos Silva
'''
# coding: latin-1
import mysql.connector
import pickle
from mysql.connector import errorcode

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
        print("dbconnected")
    def connect(self):
      try:
        self._db_connection = mysql.connector.connect(user='root', password='qwertz', host='127.0.0.1', database='pox_dstl2')
        
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
      
    def insert(self):
      insertObject = ("INSERT INTO objectdata (plugin_name, object_name, object_content) VALUES (%s, %s, %s)")
      dataObject = ('plug_example','object_example','Abstract_data_undefined_data_any_data')
     
      self._db_cursor = self._db_connection.cursor()
       
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      
      self._db_connection.commit()
      
      #test
      print(last_objectid)
      
      self._db_cursor.close()
      
    def close(self):
      self._db_connection.close()
      
    def insertObject(self, param_obj):
      print("insert Object")
      insertObject = ("INSERT INTO objectdata (plugin_name, object_name, object_content) VALUES (%s, %s, %s)")
      dataObject = ('name of plugin','name of object', param_obj)
      
      self._db_cursor = self._db_connection.cursor()
     
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      
      self._db_connection.commit()
      print(last_objectid)
      
      self._db_cursor.close()
    
    def test(self):
      print("teste")
     
        
    def serializeObject(self, param_obj):
      serialized_obj = pickle.dumps(param_obj)
      return serialized_obj
    
    def sendObject(self, pluginname, objectname, packet):
      
      so = self.serializeObject(packet.__dict__)
      #serialized_packet.pluame = "Plug xzt"
      self.insertObject(so)
      #self.insert()
      print("Sending object")
     
    