# Copyright 2017 Carlos Silva
#

"""
An Distribution app component.

It is a distribution component that store in shared database
the components and events.
"""

from pox.core import core
import pox.openflow.libopenflow_01 as of
from pox.lib.util import dpid_to_str, str_to_dpid
from pox.lib.util import str_to_bool
import time

log = core.getLogger()

class distribution ():
  def __init__(self, connection):
    # hear PacketIn messages, so we listen
    log.debug("initializing Distribution Application")
    connection.addListeners(self)

  def _handle_PacketIn(self, event):
    log.debug("Distribution Application/Core - PacketIn")
    packet = event.parsed

class distribution_core (object):
  def _handle_ConnectionUp (self, event):
    if event.dpid in self.ignore:
      log.debug("Ignoring connection %s" % (event.connection,))
      return
    log.debug("Connection %s" % (event.connection,))
    distribution(event.connection)


  def launch (self):
    core.registerNew(distribution_core)