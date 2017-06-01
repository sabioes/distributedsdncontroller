# Copyright 2012 James McCauley
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Turns your complex OpenFlow switches into stupid hubs.

There are actually two hubs in here -- a reactive one and a proactive one.
"""

from pox.core import core
import pox.openflow.libopenflow_01 as of
from pox.lib.util import dpidToStr
from pox.persistence.poxpersistence import PoxPersistence
from pox.persistence.poxpersistence import ObjectConverter

log = core.getLogger()
poxstore = PoxPersistence()

def _handle_ConnectionUp (event):
  """
  Be a proactive hub by telling every connected switch to flood all packets
  """

  store_query = poxstore.selectbyKey(event.connection.dpid, 1,"hubapp")

  if(len(store_query)==0):
    msg = ObjectConverter.deserializeObject(store_query[0][2])
    event.connection.send(msg)
  else:
    msg = of.ofp_flow_mod()
    msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
    event.connection.send(msg)
    poxstore.registPacket(event.connection.dpid, msg, "hubapp")

  log.info("Hubifying %s", dpidToStr(event.dpid))
  log.info("###################################")
  log.info("## dpid:%s ->MAC: %s ##", event.connection.dpid, event.connection.eth_addr)
  log.info("###################################")

def _handle_PacketIn (event):
  """
  Be a reactive hub by flooding every incoming packet
  """
  store_query = poxstore.selectbyKey(event.connection.dpid, 1, "hubapp")

  if (len(store_query) != 0):
    msg = ObjectConverter.deserializeObject(store_query[0][2])
    event.connection.send(msg)
  else:
    msg = of.ofp_packet_out()
    msg.data = event.ofp
    msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
    event.connection.send(msg)
    poxstore.registPacket(event.connection.dpid, msg, "hubapp")

  log.info("###################################")
  log.info("## dpid:%s ->MAC: %s ##", event.connection.dpid, event.connection.eth_addr)
  log.info("###################################")

def launch (reactive = False):
  if reactive:
    core.openflow.addListenerByName("PacketIn", _handle_PacketIn)
    log.info("Reactive hub running.")
  else:
    core.openflow.addListenerByName("ConnectionUp", _handle_ConnectionUp)
    log.info("Proactive hub running.")
