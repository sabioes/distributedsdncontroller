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
    print "storing entity"
    print entity.dpid
    last_switch = self.insertSwitch(entity.dpid)
    for port in entity.ports:
      print "Switch id:"+str(entity.dpid)+ ":port"+str(port)
      self.insertPort(port, entity.dpid)
      #self.insertSwitchEntities(last_switch, port, entity.dpid)
    #print entity.ports

  def storeLink(self, link):
    print "storing link"
    print "Port1:" + str(link.port1) + " Port2: " + str(link.port2)
    self.insertLink(link)

  def storeFlowTable(self, dpid, flowTable):
    print "storing flow table of switch"
    self.insertFlowTable(self, dpid, flowTable)

  def removeSwitch(self, switch):
    print "deleting entity"
    self.deleteSwitch(switch)
    print switch.dpid

  def removelink(self, link):
    print "deleting link"
    self.deleteLink(link.dpid1, link.dpid2)
   #self.deleteEntity()

  def removeFlowTable(self, table):
    print "deleting flow table"

  def getAllSwitchs(self):
    print " reading all switchs"
    #self.selectAll("switch")

  def getAllLinks(self):
    print "reading all links"
    self.selectAll("link")

  def getFlowTable(self, dpid):
    return "flowtable"

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
      query = ("INSERT INTO switch (dpid) VALUES (" + str(param_dpid) + ")ON DUPLICATE KEY UPDATE dpid=VALUES(dpid);")
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
                    "VALUES ("+str(param_link.port1)+","+str(param_link.dpid1)+","+str(param_link.port2)+","+str(param_link.dpid2)+")"
                    "ON DUPLICATE KEY UPDATE entity1_port=VALUES(entity1_port), entity1_dpid=VALUES(entity1_dpid), entity2_port=VALUES(entity2_port), entity2_dpid=VALUES(entity2_dpid);")

    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(insert_query)
    id = self._db_cursor.lastrowid
    self._driverConnection.commit()
    # print ID da entrada na ibase de dados
    print("Last ID inserted on link table:" + str(id))
    self._db_cursor.close()

  def insertPort(self, param_idport, param_idswitch):
    insert_query = ("INSERT INTO port(id_port, id_switch)" 
                    "VALUES("+str(param_idport)+","+ str(param_idswitch)+")"
                    "ON DUPLICATE KEY UPDATE id_port=VALUES(id_port), id_switch=VALUES(id_switch);")
    id = self._db_cursor.lastrowid
    self._driverConnection.commit()
    # print ID da entrada na ibase de dados
    print("Last ID inserted on link table:" + str(id))
    self._db_cursor.close()

  def insertSwitchEntities(self, param_id, param_idport, param_identity):
    param_id = 1
    insert_query = ("INSERT INTO switchentities(id, id_port, id_entity) VALUES(" + str(param_id) + "," + str(param_idport) + "," + str(param_identity) + ") ON DUPLICATE KEY UPDATE id=VALUES(id), id_port=VALUES(id_port), id_entity=VALUES(id_entity);")
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

      print "+++++++++++++++++++++++++++++++++++"
      print id
      print "+++++++++++++++++++++++++++++++++++"
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