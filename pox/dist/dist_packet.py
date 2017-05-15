from pox.core import core
import pox.openflow.libopenflow_01 as of
from pox.dist.externalstore import ExternalStore

log = core.getLogger()


class DistributionPacket (object):
  """
  A Tutorial object is created for each switch that connects.
  A Connection object for that switch is passed to the __init__ function.
  """
  externalstore = None
  def __init__ (self, connection):
    #
    #
    self.externalstore = ExternalStore()

    # Keep track of the connection to the switch so that we can
    # send it messages!
    self.connection = connection

    # This binds our PacketIn event listener
    connection.addListeners(self)

    # Use this table to keep track of which ethernet address is on
    # which switch port (keys are MACs, values are ports).
    self.mac_to_port = {}
  def _handle_PacketIn (self, event):
    """
    Handles packet in messages from the switch.
    """

    packet = event.parsed # This is the parsed packet data.
    if packet.parsed:
      self.externalstore.storeFloodPacket(self.__class__.__name__, "packetIn", packet)
    if not packet.parsed:
      log.warning("Ignoring incomplete packet")
      return

    packet_in = event.ofp # The actual ofp_packet_in message.

    # Comment out the following line and uncomment the one after
    # when starting the exercise.

def launch ():
  """
  Starts the component
  """
  def start_switch (event):
    log.debug("Controlling %s" % (event.connection,))
    DistributionPacket(event.connection)
  core.openflow.addListenerByName("ConnectionUp", start_switch)