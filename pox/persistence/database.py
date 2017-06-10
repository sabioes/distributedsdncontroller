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

  def createDatabase(self):
    createdatabase ='''CREATE DATABASE IF NOT EXISTS controllerdb;'''
    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(createdatabase)
    result = self._db_cursor.fetchall()
    self._db_cursor.close()

  def createTopologyTables(self):
    dropalltables='''DROP TABLE IF EXISTS controllerdb.link;
                            DROP TABLE IF EXISTS controllerdb.switchentities;
                            DROP TABLE IF EXISTS controllerdb.port;
                            DROP TABLE IF EXISTS controllerdb.switch;'''

    self._db_cursor = self._driverConnection.getCursor()
    self._db_cursor.execute(dropalltables)
    result = self._db_cursor.fetchall()

    createalltables='''CREATE TABLE IF NOT EXISTS controllerdb.switch(
                            dpid INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
                            value TEXT
                        )ENGINE=InnoDB;
                            
                        CREATE TABLE IF NOT EXISTS controllerdb.link(
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
    self._db_cursor.execute(createalltables)
    result = self._db_cursor.fetchall()
