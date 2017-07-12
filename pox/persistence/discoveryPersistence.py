from pox.persistence.driverConnection import DriverConnection

class DiscoveryPersistence():
  _driverConnection = None
  def __init__(self):
    '''
    Constructor
    '''
    self._driverConnection = DriverConnection()

  def storeLink(self, link):
    print "storing link"
    print "Port1:" + str(link.port1) + " Port2: " + str(link.port2)
    self.insertLink(link)

  def getAllLinks(self):
    print "reading all links"
    self.select("link")

  def getLink(self, param_link):
    print "Getting link by kwargs"
    self.selectlink(param_link)

  def removeLink(self, param_link):
    print "Removing link"
    self.deleteLink(param_link)


  #--> QUERY AREA
  def insertLink(self, param_link):
    insert_ms_query = ("INSERT INTO link (entity1_port, entity1_dpid, entity2_port, entity2_dpid) "
                    "VALUES ("+str(param_link.port1)+","+str(param_link.dpid1)+","+str(param_link.port2)+","+str(param_link.dpid2)+")"
                    "ON DUPLICATE KEY UPDATE entity1_port=VALUES(entity1_port), entity1_dpid=VALUES(entity1_dpid), entity2_port=VALUES(entity2_port), entity2_dpid=VALUES(entity2_dpid);")

    insert_query = ("INSERT INTO link (entity1_port, entity1_dpid, entity2_port, entity2_dpid) "
                    "VALUES ("+str(param_link.port1)+","+str(param_link.dpid1)+","+str(param_link.port2)+","+str(param_link.dpid2)+")"
                    "ON CONFLICT (entity1_port, entity1_dpid, entity2_port, entity2_dpid)  DO UPDATE "
                    "SET entity1_port = excluded.entity1_port, entity1_dpid = excluded.entity1_dpid, entity2_port = excluded.entity2_port, entity2_dpid = excluded.entity2_dpid;")

    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(insert_query)
    #id = self._db_cursor.fetchone()[0]
    #self._driverConnection.commit()
    # print ID da entrada na ibase de dados
    print("Last ID inserted on link table:" + str(id))
    self._db_cursor.close()

  def selectlink(self, param_link):
    if param_link is None:
      print "Parameter is None:"

    query = "SELECT * FROM controllerdb.link " \
            "WHERE dpid1 = " + param_link.dpid1+" AND port1 = "+param_link.port1+""\
            "AND dpid2 = "+ param_link.dpid2+" AND port2 = "+param_link.port2

    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(query)
    result = self._db_cursor.fetchall()
    self._db_cursor.close()

    if len(result) < 1:
      return None

    return result

  def removelink(self, param_link):
    if param_link is None:
      print "Param is None: returning"

    query = "DELETE FROM controllerdb.link " \
            "WHERE dpid1 = " + param_link.dpid1+" AND port1 = "+param_link.port1+""\
            "AND dpid2 = "+ param_link.dpid2+" AND port2 = "+param_link.port2

    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(query)
    result = self._db_cursor.fetchall()
    self._db_cursor.close()

    if len(result) < 1:
      return False
    return True
