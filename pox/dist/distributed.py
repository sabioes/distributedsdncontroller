'''
Created on 04/04/2017

@author: Carlos Silva
'''
# coding: latin-1
import mysql.connector
import pickle
from mysql.connector import errorcode, Error

class ObjectConverter(object):
    @staticmethod
    def serializeObject(param_obj):
      serialized_obj = pickle.dumps(param_obj)
      return serialized_obj

    @staticmethod
    def deserializeObject(param_serielizedObj):
      obj = pickle.loads(param_serielizedObj)
      return obj

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
        
      except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          print("Database does not exist")
        else:
          print(err)

    def close(self):
      self._db_connection.close()
      
    def query(self):
      query_select = "SELECT * FROM objectdata"
      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(query_select)
      result = self._db_cursor.fetchall()
      self._db_cursor.close()
      return result

    #method tests --TODELETE ALL METHOD
    def tests_insert(self):
      insertObject = ("INSERT INTO objectdata (plugin_name, object_name, object_content) VALUES (%s, %s, %s)")
      dataObject = ('plug_example','object_example','Abstract_data_undefined_data_any_data')
     
      self._db_cursor = self._db_connection.cursor()
       
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      
      self._db_connection.commit()
      
      #test
      print(last_objectid)
      
      self._db_cursor.close()
      
    def insert(self, param_plug, param_objname, param_obj):
      print("insert Object")
      insertObject = ("INSERT INTO objectdata (plugin_key, object_key, object_value) VALUES (%s, %s, %s)")
      dataObject = (param_plug, param_objname, param_obj)
      
      self._db_cursor = self._db_connection.cursor()
     
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      
      self._db_connection.commit()
      print(last_objectid)
      
      self._db_cursor.close()

    def delete(self, id):
      query = "DELETE FORM objectdata WHERE id = %s"
      try:
        self._db_cursor = self._db_connection.cursor()
        self._db_cursor.execute(id)

      except Error as error:
        print(error)

      finally:
        self._db_cursor.close()

    def test(self):
      print("test")

    def existingObjects(self, param_object_name, param_object_value):
      #change by log
      print("Doing Existing Objects")

      existarray = []

      result_query = self.query()
      serialized_param_object_value = ObjectConverter.serializeObject(param_object_value)

      for (id_data, plugin_key, object_key, object_value) in result_query:
        unserialize_object=ObjectConverter.deserializeObject(object_value)
        print(unserialize_object)
        if param_object_name == object_key and serialized_param_object_value == object_value:
          existarray.append(id_data)


        #print("id {}, has {} | {} | {}".format(id_data, plugin_key, object_key, object_value))
        print("----------------------------------------------------------")
        print("{}".format(object_value))
        print("----------------------------------------------------------")

      return existarray

    def storeObject(self, pluginname, objectname, packet):
      print("Sending object")

      so = ObjectConverter.serializeObject(packet)
      #serialized_packet.pluame = "Plug xzt"

      if(len(self.existingObjects(objectname, packet)) < 1):
        self.insert(pluginname, objectname, so)

      #self.existingObjects(objectname, packet)

    def dropObject(self, pluginname, objectname, packet):
      print("Droping object")


    