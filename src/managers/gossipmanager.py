import asyncio

from src.avails import PalmTreeInformResponse, Wire, WireData, connect, const, use
from src.core import get_this_remote_peer
from src.core.transfers import PalmTreeProtocol, PalmTreeRelay, PalmTreeSession


class GossipSessionRegistry:
    current_sessions = {}
    completed_session = []

    @classmethod
    def add_session(cls, mediator):
        cls.current_sessions[mediator.session.id] = mediator

    @classmethod
    def get_session(cls, session_id) -> PalmTreeRelay:
        return cls.current_sessions.get(session_id, None)

    @classmethod
    def remove_session(cls, session_id):
        del cls.current_sessions[session_id]


async def new_gossip_request_arrived(req_data: WireData, addr):
    loop = asyncio.get_event_loop()
    connection = await connect.UDPProtocol.create_connection_async(loop, addr)
    stream_endpoint_addr = get_active_endpoint_address()
    datagram_endpoint, datagram_endpoint_addr = get_passive_endpoint(addr, loop)
    session = PalmTreeSession(
        originater_id=req_data.id,
        adjacent_peers=req_data['adjacent_peers'],
        session_id=req_data['session_id'],
        key=req_data['session_key'],
        fanout=req_data['max_forwards'],
        link_wait_timeout=PalmTreeProtocol.request_timeout,
        chunk_size=1024,
    )
    response = PalmTreeInformResponse(
        peer_id=get_this_remote_peer().id,
        active_addr=stream_endpoint_addr,
        passive_addr=datagram_endpoint_addr,
        session_key=req_data['session_key']
    )
    schedule_gossip_session(session, datagram_endpoint, stream_endpoint_addr)
    Wire.send_datagram(connection, addr, bytes(response))


def get_active_endpoint_address():
    return get_this_remote_peer().uri


def get_passive_endpoint(addr, loop):
    datagram_endpoint_addr = (get_this_remote_peer().ip, connect.get_free_port())
    datagram_endpoint = connect.UDPProtocol.create_async_server_sock(
        loop,
        addr,
        family=const.IP_VERSION,
        backlog=3
    )
    return datagram_endpoint, datagram_endpoint_addr


def get_active_endpoint_socket1():
    loop = asyncio.get_event_loop()
    stream_endpoint_addr = (get_this_remote_peer().ip, connect.get_free_port())
    stream_endpoint = connect.TCPProtocol.create_async_server_sock(
        loop,
        stream_endpoint_addr,
        family=const.IP_VERSION,
        backlog=3
    )
    return stream_endpoint, stream_endpoint_addr


def schedule_gossip_session(session, passive_sock, active_endpoint_addr):
    session_mediator = PalmTreeRelay(session, passive_sock, active_endpoint_addr)
    f = use.wrap_with_tryexcept(session_mediator.session_init)
    session_mediator.session_task = asyncio.create_task(f())
    GossipSessionRegistry.add_session(mediator=session_mediator)


async def update_gossip_stream_socket(connection, link_data):
    session_id = link_data['session_id']
    mediator = GossipSessionRegistry.get_session(session_id)
    await mediator.gossip_add_stream_link(connection, link_data)