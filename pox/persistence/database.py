import select
from mysql.connector import errorcode, Error
from pox.persistence.driverConnection import DriverConnection
from pox.openflow.topology import *

class DatabaseInitiator():
  _driverConnection = None
  def __init__(self):
    '''
    Constructor
    '''
    self._driverConnection = DriverConnection()

  def createTopologyTables(self):
    try:
      dropalltables='''DROP TABLE `controllerdb`.`link`;'''

      self._db_cursor = self._driverConnection.getCursor()
      self._db_cursor.execute(dropalltables, multi=True)
      self._driverConnection.commit()
      self._db_cursor.close()

    except self._driverConnection.connector.Error as err:
      print("Failed Droping tables: {}".format(err))
      exit(1)

    try:
      createalltables='''CREATE TABLE IF NOT EXISTS controllerdb.switch(
                            dpid INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
                            value TEXT
                        )ENGINE=InnoDB;
                            
                        CREATE TABLE IF NOT EXISTS `controllerdb`.`link`(
                          id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
                            entity1_port INT,
                            entity1_dpid INT,
                            entity2_port INT,
                            entity2_dpid INT,
                            FOREIGN KEY (entity1_dpid) REFERENCES controllerdb.switch (dpid),
                            FOREIGN KEY (entity2_dpid) REFERENCES controllerdb.switch (dpid)
                        )ENGINE=InnoDB;
                        
                        CREATE TABLE IF NOT EXISTS controllerdb.port(
                            id_port int,
                            id_switch INT,
                            FOREIGN KEY (id_switch) REFERENCES controllerdb.switch (dpid),
                            PRIMARY KEY (id_port, id_switch)
                        )ENGINE=InnoDB;
                        
                        CREATE TABLE IF NOT EXISTS controllerdb.switchentities(
                          id int,
                            id_port int,
                            FOREIGN KEY (id_port) REFERENCES controllerdb.port(id_port)
                        )ENGINE=InnoDB; '''

      self._db_cursor = self._driverConnection.getCursor()
      self._db_cursor.execute(createalltables, multi=True)
      self._db_cursor.close()

    except self._driverConnection.connector.Error as err:
      print("Failed creating tables: {}".format(err))
      exit(1)
