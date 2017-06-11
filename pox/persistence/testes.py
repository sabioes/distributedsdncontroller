from pox.persistence.driverConnection import DriverConnection

def createTopologyTables(self):
  try:
    driver = DriverConnection()
    dropalltables = '''DROP TABLE link;'''

    self._db_cursor = self.driver.getCursor()
    self._db_cursor.execute(dropalltables, multi=True)
    self.driver.commit()
    self._db_cursor.close()

  except self._driverConnection.connector.Error as err:
    print("Failed Droping tables: {}".format(err))
    exit(1)

createTopologyTables()