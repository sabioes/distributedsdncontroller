import select
from mysql.connector import errorcode, Error
from pox.persistence.driverConnection import DriverConnection
from pox.openflow.topology import *

class OpenflowTopologyPersistence():
  _driverConnection = None
  def __init__(self):
    '''
    Constructor
    '''
    self._driverConnection = DriverConnection()

  def storeSwitch(self, entity):
    print "a guardar entity"
    print entity.dpid
    last_switch = self.insertSwitch(entity.dpid)
    for port in entity.ports:
      print "Switch id:"+str(entity.dpid)+ ":port"+str(port)
      self.insertPort(port, entity.dpid)
      self.insertSwitchEntities(id, port)
    #print entity.ports

  def storeLink(self, link):
    print "a guardar link"
    print "Port1:" + str(link.port1) + " Port2: " + str(link.port2)
    self.insertLink(link)

  def deleteEntity(self, entity):
    print "a apagar entity"
    print entity.dpid

  def deletelink(self, link):
    print "a apagar link"
    self.deleteEntity()

  def getAllSwitchs(self):
    print " a ler todos os switchs"
    #self.selectAll("switch")

  def getAllLinks(self):
    print "a ler todos os links"
    #self.selectAll("link")

  def selectAll(self, param=None):
    if param is None:
      print "algo se passou, porque o parametro e None"

    query = "SELECT * FROM " + param
    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(query)
    result = self._db_cursor.fetchall()
    self._db_cursor.close()

    if len(result) < 1:
      return None

    return result

  def selectbydpid(self, param_dpid):
    if param_dpid is None:
      print "algo se passou, porque o parametro e None"
      return
    query = "SELECT * FROM switch WHERE dpid = " +str(param_dpid)
    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(query)
    result = self._db_cursor.fetchall()
    self._db_cursor.close()
    return result

  def insertSwitch(self, param_dpid):
    if len(self.selectbydpid(param_dpid))>=1:
      return
    else:
      query = ("INSERT INTO switch (dpid) VALUES (" + str(param_dpid) + ")")
      self._db_cursor = self._driverConnection.getCursor()
      self._db_cursor.execute(query)
      dpid = self._db_cursor.lastrowid
      self._driverConnection.commit()
      # print ID da entrada na base de dados
      print("Last ID inserted on entity table:" + str(dpid))
      self._db_cursor.close()
      return dpid

  def insertLink(self, param_link):
    insert_query = ("INSERT INTO link (entity1_port, entity1_dpid, entity2_port, entity2_dpid) "
                    "VALUES ("+str(param_link.port1)+","+str(param_link.dpid1)+","+str(param_link.port1)+","+str(param_link.dpid2)+")")
    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(insert_query)
    id = self._db_cursor.lastrowid
    self._driverConnection.commit()
    # print ID da entrada na ibase de dados
    print("Last ID inserted on link table:" + str(id))
    self._db_cursor.close()

  def insertPort(self, param_idport, param_idswitch):
    insert_query = ("INSERT INTO port(id_port, id_switch)" 
                    "VALUES("+str(param_idport)+","+ str(param_idswitch)+")")
    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(insert_query)
    id = self._db_cursor.lastrowid
    self._driverConnection.commit()
    # print ID da entrada na ibase de dados
    print("Last ID inserted on link table:" + str(id))
    self._db_cursor.close()

  def insertSwitchEntities(self, param_id, param_port):
    insert_query = ("INSERT INTO switchesEntities(param_id, param_port)"
                    "VALUES(" + str(param_id) + "," + str(param_port) + ")")
    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(insert_query)
    id = self._db_cursor.lastrowid
    self._driverConnection.commit()
    # print ID da entrada na ibase de dados
    print("Last ID inserted on link table:" + str(id))
    self._db_cursor.close()

  def deleteSwitch(self, param_dpid):
    query = ("DELETE FORM switch WHERE id ="+str(param_dpid))
    try:
      self._db_cursor = self._driverConnection.getCursor()
      self._db_cursor.execute(id)

    except Error as error:
      print(error)

    finally:
      self._db_cursor.close()


  def deleteLink(self, param_dpid1, param_dpid2):
    query = ("DELETE FORM link WHERE entity1_dpid ="+str(param_dpid1)+" AND entity2_dpid ="+str(param_dpid2))
    try:
      self._db_cursor = self._driverConnection.getCursor()
      self._db_cursor.execute(id)

    except Error as error:
      print(error)

    finally:
      self._db_cursor.close()