import webbrowser
import websockets
import configparser

import src.avails.textobject
import src.core.senders
import src.managers.endmanager
from src.core import *
from src.core.senders import RecentConnections, send_message, send_file, send_file_with_window, send_dir_with_window
from src.avails.remotepeer import RemotePeer
from src.avails import useables as use
from src.avails.textobject import DataWeaver
from src.managers import directorymanager
from src.core import requests_handler as req_handler
from src import avails


web_socket: websockets.WebSocketServerProtocol
server_data_lock = threading.Lock()
SafeEnd = asyncio.Event()
stack_safe = threading.Lock()


async def handle_connection(addr_id):
    try:
        _nomad: RemotePeer = use.get_peer_obj_from_id(addr_id)
    except KeyError:
        print("Looks like the user is not in the list can't connect to the user")
        return False
    RecentConnections.addConnection(peer_obj=_nomad)


async def command_flow_handler(data_in: DataWeaver):
    if data_in.match(_content=const.HANDLE_END):
        src.managers.endmanager.end_session()
    elif data_in.match(_content=const.HANDLE_CONNECT_USER):
        await handle_connection(addr_id=data_in.id)
    elif data_in.match(_content=const.HANDLE_POP_DIR_SELECTOR):
        await send_dir_with_window(_path=data_in.content, user_id=data_in.id)
    elif data_in.match(_content=const.HANDLE_PUSH_FILE_SELECTOR):
        await send_file_with_window(_path=data_in.content, user_id=data_in.id)
    elif data_in.match(_content=const.HANDLE_OPEN_FILE):
        use.open_file(data_in.content)
    elif data_in.match(const.HANDLE_RELOAD):
        use.reload_protocol()
        return
    elif data_in.match(const.HANDLE_SYNC_USERS):
        use.start_thread(_target=req_handler.sync_list)


async def control_data_flow(data_in: DataWeaver):
    """
    A function to control the data flow from the page
    :param data_in:
    :return:
    """
    function_map = {
        const.HANDLE_COMMAND: lambda x: command_flow_handler(x),
        const.HANDLE_MESSAGE_HEADER: lambda x: send_message(x),
        const.HANDLE_FILE_HEADER: lambda x: send_file(x),
        const.HANDLE_DIR_HEADER: lambda x: send_file(x),
        const.HANDLE_DIR_HEADER_LITE: lambda x: directorymanager.directory_sender(
            receiver_obj=const.LIST_OF_PEERS[x.id], dir_path=x.content),
    }
    try:
        await function_map.get(data_in.header, lambda x: None)(data_in)
    except TypeError as exp:
        error_log(f"Error at handle/control_data_flow : {exp} due to {data_in.header}")


# @NotInUse
async def set_name(new_username):
    config = configparser.ConfigParser()
    config.read(const.PATH_CONFIG)
    config.set('CONFIGURATIONS', 'username', new_username)
    const.USERNAME = new_username


async def getdata():
    global web_socket, SafeEnd
    while not SafeEnd.is_set():
        # try:
        raw_data = await web_socket.recv()
        data = DataWeaver(byte_data=raw_data)
        with const.LOCK_PRINT:
            print("data from page:", data)
        await control_data_flow(data_in=data)
        # except Exception as e:
        #     print(f"Error in getdata: {e} at handle_data.py/getdata() ")
        #     break
    print('::SafeEnd is set')


async def handler(_websocket):
    global web_socket, SafeEnd
    web_socket = _websocket
    if const.USERNAME == '':
        userdata = DataWeaver(header="thisisacommand",
                              content="no..username", )
    else:
        userdata = DataWeaver(header="thisismyusername",
                              content=f"{const.USERNAME}(^){const.THIS_IP}",
                              _id='0')
    await web_socket.send(userdata.dump())
    const.LOCK_FOR_PAGE = True
    const.WEB_SOCKET = web_socket
    const.PAGE_HANDLE_CALL.set()
    await getdata()
    use.echo_print(True, '::handler ended')


def initiate_control():
    use.echo_print(True, '::Initiate_control called at handle_data.py :', const.PATH_PAGE, const.PORT_PAGE)
    # use.start_thread(_target=httphandler.start_serving, _args=())
    # webbrowser.open(f"http://localhost:{const.PAGE_SERVE_PORT}")
    webbrowser.open(os.path.join(const.PATH_PAGE, "index.html"))
    asyncio.set_event_loop(asyncio.new_event_loop())
    start_server = websockets.serve(handler, "localhost", const.PORT_PAGE)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


async def feed_user_data_to_page(_data: str, ip):
    global web_socket
    _data = DataWeaver(header="thisismessage",
                       content=f"{_data}",
                       _id=f"{ip}")
    try:
        print(f"::Sending data :{_data} to page: {ip}")
        await web_socket.send(_data.dump())
    except Exception as e:
        error_log(f"Error sending data handle_data.py/feed_user_data exp: {e}")
        return


async def feed_core_data_to_page(data: DataWeaver):
    global web_socket
    try:
        print(f"::Sending data :{data} \n to page")
        await web_socket.send(data.dump())
    except Exception as e:
        error_log(f"Error sending data handle_data.py/feed_core_data exp: {e}")
        return


async def feed_server_data_to_page(peer: avails.remotepeer.RemotePeer):
    global web_socket, server_data_lock
    with server_data_lock:
        _data = DataWeaver(header=const.HANDLE_COMMAND,
                           content=(peer.username if peer.status else 0),
                           _id=peer.id)
        try:
            use.echo_print(False, f"::Sending data :{_data} to page: {peer.username}")
            await web_socket.send(_data.dump())
        except Exception as e:
            error_log(f"Error sending data at handle_data.py/feed_server_data, exp: {e}")
        pass


def end():
    global SafeEnd, web_socket
    if web_socket is None:
        return None
    SafeEnd.set()
    asyncio.get_event_loop().stop() if asyncio.get_event_loop().is_running() else asyncio.get_event_loop().close()
    if asyncio.get_running_loop().is_running():
        asyncio.get_running_loop().stop()
        asyncio.get_running_loop().close()
    use.echo_print(True, "::Handle Ended")
    return