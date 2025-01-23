import asyncio
import logging
from typing import TYPE_CHECKING

from src.avails import DataWeaver, WireData, const, use
from src.avails.bases import BaseDispatcher
from src.avails.events import RequestEvent
from src.avails.mixins import QueueMixIn, ReplyRegistryMixIn
from src.core import Dock, get_this_remote_peer
from src.transfers import DISCOVERY
from src.transfers.transports import DiscoveryTransport
from src.webpage_handlers import headers, pagehandle

_logger = logging.getLogger(__name__)


def DiscoveryReplyHandler(kad_server):
    async def handle(event: RequestEvent):
        if event.from_addr[0] == const.THIS_IP:
            return
        connect_address = tuple(event.request["connect_uri"])
        _logger.debug(f"bootstrapping kademlia {connect_address}")
        if any(await kad_server.bootstrap([connect_address])):
            _logger.debug("bootstrapping completed")

    return handle


def DiscoveryRequestHandler(discovery_transport):
    async def handle(event: RequestEvent):
        req_packet = event.request
        if req_packet["reply_addr"][0] == const.THIS_IP:
            _logger.debug("ignoring echo")
            return
        _logger.info(f"discovery replying to req: {req_packet.body}")
        this_rp = get_this_remote_peer()
        data_payload = WireData(
            header=DISCOVERY.NETWORK_FIND_REPLY,
            msg_id=this_rp.peer_id,
            connect_uri=this_rp.req_uri,
        )
        discovery_transport.sendto(
            bytes(data_payload), tuple(req_packet["reply_addr"])
        )

    return handle


class DiscoveryDispatcher(QueueMixIn, ReplyRegistryMixIn, BaseDispatcher):
    __slots__ = ()
    if TYPE_CHECKING:
        transport: DiscoveryTransport

    def __init__(self, transport, stopping_flag):
        super().__init__(
            transport=DiscoveryTransport(transport), stop_flag=stopping_flag
        )

    async def submit(self, event: RequestEvent):
        wire_data = event.request
        self.msg_arrived(wire_data)
        handle = self.registry[wire_data.header]
        _logger.debug(f"dispatching request {handle}")
        await handle(event)


async def send_discovery_requests(transport, broad_cast_addr, multicast_addr):
    this_rp = get_this_remote_peer()
    ping_data = bytes(
        WireData(
            DISCOVERY.NETWORK_FIND,
            this_rp.peer_id,
            reply_addr=this_rp.req_uri
        )
    )

    async def send_discovery_packet():
        if const.USING_IP_V4:
            async for _ in use.async_timeouts(initial=0.1, max_retries=const.DISCOVER_RETRIES):
                transport.sendto(ping_data, broad_cast_addr)
                # transport.transport._sock.sendto(ping_data,broad_cast_addr)
            _logger.debug(f"sent discovery request to broadcast {broad_cast_addr}", )

        async for _ in use.async_timeouts(initial=0.1, max_retries=const.DISCOVER_RETRIES):
            transport.sendto(ping_data, multicast_addr)
        _logger.debug(f"sent discovery request to multicast {multicast_addr}")

    async def enter_passive_mode():
        _logger.info(f"entering passive mode for discovery after waiting for {const.DISCOVER_TIMEOUT}s")
        async for _ in use.async_timeouts(initial=0.1, max_retries=-1, max_value=const.DISCOVER_TIMEOUT):
            if Dock.kademlia_network_server.is_bootstrapped:
                _logger.info(f"{Dock.kademlia_network_server.is_bootstrapped=}")
                continue
            if Dock.finalizing.is_set():
                return

            await send_discovery_packet()

    await send_discovery_packet()

    await asyncio.sleep(const.DISCOVER_TIMEOUT)  # wait a bit
    # stay in passive mode and keep sending discovery requests

    task = asyncio.create_task(enter_passive_mode())

    # try requesting user a host name of peer that is already in network
    if not Dock.kademlia_network_server.is_bootstrapped:
        _logger.debug(f"requesting user for peer name after waiting for {const.DISCOVER_TIMEOUT}s")
        await _try_asking_user(transport, ping_data)

    await task


async def _try_asking_user(transport, discovery_packet):
    reply = await pagehandle.dispatch_data(
        DataWeaver(
            header=headers.REQ_PEER_NAME_FOR_DISCOVERY,
        ),
        expect_reply=True,
    )

    if not reply.content['peername']:
        return

    peer_name = reply.content['peername']
    async for family, sock_type, proto, _, addr in use.get_addr_info(peer_name, const.PORT_REQ):
        transport.sendto(discovery_packet)
