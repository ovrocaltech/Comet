# Comet VOEvent Broker.
# John Swinbank, <swinbank@transientskp.org>, 2012.

# Python standard library
import os
import sys

# Used for building IP whitelist
from ipaddr import IPNetwork

# Twisted
from twisted.internet import reactor
from twisted.python import log
from twisted.python import usage
from twisted.application.service import MultiService
from twisted.application.internet import TCPClient
from twisted.application.internet import TCPServer

# Comet broker routines
import comet
from ..config.options import BaseOptions
from ..tcp.protocol import VOEventPublisherFactory
from ..tcp.protocol import VOEventReceiverFactory
from ..tcp.protocol import VOEventSubscriberFactory
from ..utility.relay import EventRelay
from ..utility.whitelist import WhitelistingFactory
from ..utility.schemavalidator import SchemaValidator
from ..utility.ivorn_db import CheckPreviouslySeen
from ..utility.ivorn_db import IVORN_DB

class Options(BaseOptions):
    optParameters = [
        ["receiver-port", "r", 8098, "TCP port for receiving events.", int],
        ["subscriber-port", "p", 8099, "TCP port for publishing events.", int],
        ["ivorndb", "i", "/tmp", "IVORN database root."],
        ["whitelist", None, None, "Network to be included in submission whitelist (CIDR)."],
        ["remote", None, None, "Remote broker to subscribe to (host:port)."],
    ]

    def __init__(self):
        BaseOptions.__init__(self)
        self['whitelist'] = []
        self['remotes'] = []

    def opt_whitelist(self, network):
        reactor.callWhenRunning(log.msg, "Accepting submissions from %s" % network)
        self['whitelist'].append(IPNetwork(network))

    def opt_remote(self, remote):
        reactor.callWhenRunning(log.msg, "Subscribing to remote broker %s" % remote)
        host, port = remote.split(":")
        self['remotes'].append((host, int(port)))


class WhitelistingReceiverFactory(VOEventReceiverFactory, WhitelistingFactory):
    def __init__(self, local_ivo, whitelist, validators=[], handlers=[]):
        VOEventReceiverFactory.__init__(self, local_ivo, validators, handlers)
        WhitelistingFactory.__init__(self, whitelist)


def makeService(config):
    ivorn_db = IVORN_DB(config['ivorndb'])

    broker_service = MultiService()
    publisher_factory = VOEventPublisherFactory(config["local-ivo"])
    TCPServer(
        config['subscriber-port'],
        publisher_factory
    ).setServiceParent(broker_service)

    TCPServer(
        config['receiver-port'],
        WhitelistingReceiverFactory(
            local_ivo=config["local-ivo"],
            whitelist=config["whitelist"],
            validators=[
                CheckPreviouslySeen(ivorn_db),
                SchemaValidator(
                    os.path.join(comet.__path__[0], "schema/VOEvent-v2.0.xsd")
                )
            ],
            handlers=[EventRelay(publisher_factory)]
        )
    ).setServiceParent(broker_service)

    for host, port in config["remotes"]:
        TCPClient(
            host, port,
            VOEventSubscriberFactory(
                local_ivo=config["local-ivo"],
                validators=[CheckPreviouslySeen(ivorn_db)],
                handlers=[EventRelay(publisher_factory)]
            )
        ).setServiceParent(broker_service)
    return broker_service
