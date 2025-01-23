import asyncio
import getpass
import os
import random
import sys
from typing import NamedTuple

from kademlia import utils

import _path  # noqa
import src
from src import managers
from src.avails import RemotePeer, const
from src.avails.connect import UDPProtocol
from src.configurations import bootup, configure
from src.core import Dock, connections, requests, set_current_remote_peer_object
from src.managers import profilemanager
from src.managers.statemanager import State, StateManager


def _create_listen_socket_mock(bind_address, multicast_addr):
    loop = asyncio.get_running_loop()
    sock = UDPProtocol.create_async_server_sock(
        loop, bind_address, family=const.IP_VERSION
    )
    print("mocked socket creation")
    return sock


# async def setup_request_transport(bind_address, multicast_address, req_dispatcher):
#     loop = asyncio.get_running_loop()
#     base_socket = _create_listen_socket(bind_address, multicast_address)
#     transport, proto = await loop.create_datagram_endpoint(
#         functools.partial(RequestsEndPoint, req_dispatcher),
#         sock=base_socket
#     )
#     req_dispatcher.transport = RequestsTransport(transport)
#     return transport


def test():
    requests._create_listen_socket = _create_listen_socket_mock
    const.THIS_IP = '127.0.0.' + sys.argv[1]
    const.SERVER_IP = const.THIS_IP
    const.MULTICAST_IP_v4 = '127.0.0.1'
    const.PORT_NETWORK = 4000
    const.DISCOVER_RETRIES = 1
    set_current_remote_peer_object(
        RemotePeer(
            byte_id=utils.digest(f"{const.THIS_IP}{const.PORT_THIS}"),
            username=f"test-{getpass.getuser()}",
            ip=const.THIS_IP,
            conn_port=const.PORT_THIS,
            req_port=const.PORT_REQ,
            status=1,
        )
    )
    # print(peers.get_this_remote_peer())


def get_a_peer() -> RemotePeer | None:
    try:
        p = next(iter(Dock.peer_list))
    except StopIteration:
        print("no peers available")
        return None
    return p


def profile_getter():
    return NamedTuple('MockProfile', (('id', int), ('username', str)))(random.getrandbits(255), getpass.getuser())


def mock_profile():
    src.managers.profilemanager._current_profile = profile_getter()


def mock_multicast():
    const.MULTICAST_IP_v4 = "172.16.210.0"
    # const.MULTICAST_IP_v4 = '172.16.196.238'
    const.PORT_NETWORK = 4000
    requests._create_listen_socket = _create_listen_socket_mock


def test_initial_states():
    s1 = State("setpaths", configure.set_paths)
    s2 = State("boot_up initiating", bootup.initiate_bootup)
    s3 = State("mocking up multicast", mock_multicast)
    # s3 = State("adding shit", test)
    s4 = State("loading profiles", profilemanager.load_profiles_to_program)
    s5 = State("mocking profile", mock_profile)
    s6 = State("configuring this remote peer object", bootup.configure_this_remote_peer)
    s7 = State("printing configurations", configure.print_constants)
    s8 = State("intitating requests", requests.initiate)
    s9 = State("initiating comms", connections.initiate_connections, is_blocking=True)
    return tuple(locals().values())


async def initiate(states):
    Dock.state_manager_handle = StateManager()
    await Dock.state_manager_handle.put_states(states)
    await Dock.state_manager_handle.process_states()


if __name__ == '__main__':
    # print(isinstance(RequestsDispatcher, AbstractDispatcher))
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    const.debug = False
    asyncio.run(initiate(test_initial_states()), debug=True)
