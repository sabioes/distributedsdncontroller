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
      return pickle.dumps(param_obj)

    @staticmethod
    def deserializeObject(param_serielizedObj):
      return pickle.loads(param_serielizedObj)


class PoxPersistence(object):
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

####################################### first version connections ######################################################

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





#####################################################################################################
######################### SECOND VERSION DATABASE ###################################################

    def query(self, param_table_name):
      query_select = "SELECT * FROM "+ param_table_name
      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(query_select)
      result = self._db_cursor.fetchall()
      self._db_cursor.close()
      return result

    ##################################### SELECT BY NAME ############################################
    def selectbyKey(self, param_key, param_limit, param_table_name):
      query_select = "SELECT * FROM "+param_table_name+" WHERE objkey="+str(param_key)+" LIMIT "+str(param_limit)
      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(query_select)
      result = self._db_cursor.fetchall()
      self._db_cursor.close()
      return result

    def insertkeyvalue(self, param_objkey, param_objvalue, param_table_name):
      logging.info("insert Object "+param_table_name)
      insertObject = ("INSERT INTO "+param_table_name+" (objkey, objvalue) VALUES (%s, %s)")
      dataObject = (param_objkey, param_objvalue)
      self._db_cursor = self._db_connection.cursor()
      self._db_cursor.execute(insertObject, dataObject)
      last_objectid = self._db_cursor.lastrowid
      self._db_connection.commit()
      # print ID da entrada na base de dados
      print("Last ID inserted on "+param_table_name+" database:" + str(last_objectid))
      self._db_cursor.close()

    def existingObjects(self, param_object_key, param_objectvalue, param_table_name):
      logging.info("Confirm existing Component on store.")
      existarray = []
      result_query = self.query(param_table_name)
      #serialized_param_object_value = ObjectConverter.unserializeObject(param_objectvalue_serialized)
      for (id, objkey, objvalue) in result_query:
        unserializedkey = ObjectConverter.deserializeObject(objkey)
        if param_object_key == unserializedkey and param_objectvalue == objvalue:
          existarray.append(id)
      return existarray

    def registPacket(self, key, value, table_name):
      logging.info("Registing PacktIN.")
      valueserielized = ObjectConverter.serializeObject(value)
      #valuestr = str(value)

      #print valuestr
      #if (len(self.existingObjects(keyserielized, value, table_name)) < 1):
        #self.insertkeyvalue(keyserielized, value, table_name)
      self.insertkeyvalue(key, valueserielized, table_name)


######################### DISTRIBUTION DATABASE TOPOLOGY ############################################
    def registObject(self, para_key, param_name, param_topology):
      logging.info("Persistence regist Object.")
      self.insertkeyvalue(self, para_key, param_name, param_topology)

    def deleteObject(self, param_key, param_table):
      query = "DELETE FORM {} WHERE id = {}".format(param_table, param_key)
      try:
        self._db_cursor = self._db_connection.cursor()
        self._db_cursor.execute(id)

      except Error as error:
        print(error)

      finally:
        self._db_cursor.close()

