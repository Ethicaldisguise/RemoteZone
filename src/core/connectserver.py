import asyncio
import queue
import time

from .peers import peer_list
from src.avails import *

from src.core import get_this_remote_peer


async def get_initial_list(no_of_users, initiate_socket):
    ping_queue = queue.Queue()
    for _ in range(no_of_users):
        # try:
        _nomad = await RemotePeer.deserialize(initiate_socket)
        ping_queue.put(_nomad)
        # requests_handler.signal_status(ping_queue, )
        use.echo_print(f"::User received from server :\n {_nomad}")
    # except socket.error as e:
    #     error_log('::Exception while receiving list of users at connect server.py/get_initial_list, exp:' + str(e))
    #     if not e.errno == 10054:
    #         continue
    #
    #     send_quit_status_to_server()
    #     if len(peer_list) > 0:
    #         server_log(f"::Server disconnected received some users retrying ...", 4)
    #         list_error_handler()
    #     return False
    return True


async def get_list_from(initiate_socket):
    with initiate_socket:
        raw_length = await initiate_socket.arecv(8)
        length = struct.unpack('!Q', raw_length)[0]  # number of users
        return await get_initial_list(length, initiate_socket)


async def list_error_handler():
    req_peer = next(peer_list)
    # try:
    conn = await connect.connect_to_peer(_peer_obj=req_peer, to_which=connect.REQ_URI)
    # except OSError:
    with conn:
        request = SimplePeerBytes(refer_sock=conn, data=const.REQ_FOR_LIST)
        await request.send()
        list_len = struct.unpack('!Q',await conn.arecv(8))[0]
        await get_initial_list(list_len, conn)


async def list_from_forward_control(list_owner: RemotePeer):
    # try:
    conn = await connect.connect_to_peer(_peer_obj=list_owner, to_which=connect.REQ_URI)
    # except:

    with conn as list_connection_socket:
        await SimplePeerBytes(list_connection_socket, const.REQ_FOR_LIST).send()
        await get_list_from(list_connection_socket)


async def initiate_connection():
    use.echo_print(f"::Connecting to server {const.SERVER_IP}${const.PORT_SERVER}")
    server_connection = await setup_server_connection()
    if server_connection is None:
        use.echo_print("\n::Can't connect to server")
        return False
    with server_connection:
        text = SimplePeerBytes(server_connection)
        if await text.receive(cmp_string=const.SERVER_OK, require_confirmation=False):
            use.echo_print('\n::Connection accepted by server')
            await get_list_from(server_connection)
        elif text.__eq__(const.REDIRECT):
            # server may send a peer's details to get list from
            recv_list_user = await RemotePeer.deserialize(server_connection)
            use.echo_print('::Connection redirected by server to : ', recv_list_user.req_uri)
            await list_from_forward_control(recv_list_user)
        else:
            return None
        return True


async def setup_server_connection():
    address = (const.SERVER_IP, const.PORT_SERVER)
    conn = None
    for i, timeout in enumerate(use.get_timeouts(0.1)):
        try:
            conn = await connect.create_connection_async(address, timeout=const.SERVER_TIMEOUT)
            break
        except asyncio.TimeoutError:
            what = f" {f'retrying... {i}'}"
            print(f"\r::Connection refused by server, {what}", end='')
            time.sleep(timeout)
        except KeyboardInterrupt:
            return
    if conn is None:
        return
    try:
        await get_this_remote_peer().send_serialized(conn)
    except (socket.error, OSError):
        conn.close()
        return
    return conn


async def send_quit_status_to_server():
    try:
        get_this_remote_peer().status = 0
        sock = await connect.create_connection_async(
            (const.SERVER_IP, const.PORT_SERVER),
            timeout=const.SERVER_TIMEOUT
        )
        with sock:
            get_this_remote_peer().send_serialized(sock)
        use.echo_print("::sent leaving status to server")
        return True
    except Exception as exp:
        print(f"at {use.func_str(send_quit_status_to_server)}", exp)
        # server_log(f'::Failed disconnecting from server at {__name__}/{__file__}, exp : {exp}', 4)
        return False
