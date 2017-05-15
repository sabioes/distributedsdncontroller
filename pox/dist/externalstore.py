'''
Created on 04/04/2017

@author: Carlos Silva
'''

# from pox.core import core
import pickle
import logging
import mysql.connector
from mysql.connector import errorcode, Error


# log = core.getLogger()

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


      print(last_objectid)

      self._db_cursor.close()

    def insert(self, param_plug, param_objname, param_obj):
      logging.info("insert Object")
      insertObject = ("INSERT INTO objectdata (plugin_key, object_key, object_value) VALUES (%s, %s, %s)")
      dataObject = (param_plug, param_objname, param_obj)

      self._db_cursor = self._db_connection.cursor()

      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid

      self._db_connection.commit()
      #print ID da entrada na base de dados
      print("Last ID inserted on database"+last_objectid)

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

    def existingObjects(self, param_object_name, param_object_value):
      logging.info("Confirm existing packet on store.")

      existarray = []

      result_query = self.query()
      serialized_param_object_value = ObjectConverter.serializeObject(param_object_value)

      for (id_data, plugin_key, object_key, object_value) in result_query:
        #tests
        #unserialize_object=ObjectConverter.deserializeObject(object_value)
        #print(unserialize_object)
        if param_object_name == object_key and serialized_param_object_value == object_value:
          existarray.append(id_data)

      return existarray

    def storeFloodPacket(self, pluginname, objectname, packet):
      logging.info("Storing packet from store.")
      so = ObjectConverter.serializeObject(packet)
      if(len(self.existingObjects(objectname, packet)) < 1):
        self.insert(pluginname, objectname, so)

    def dropFloodPacket(self, pluginname, objectname, packet):
      logging.info("Droping packet from store.")
      so = ObjectConverter.serializeObject(packet)
      lst_object = self.existingObjects(objectname, packet)
      if(len(lst_object) >= 1):
        for object in lst_object:
          #print object
          self.delete(object)

    def registObject(self, pluginname, name, component):
      logging.info("Registing components.")
      so = ObjectConverter.serializeObject(component)
      if (len(self.existingObjects(name, component)) < 1):
        self.insert(pluginname, name, so)

