import asyncio
import asyncio as _asyncio
import importlib
import itertools
import threading
from typing import Optional, Union

import websockets

from src.avails import DataWeaver, WireData, const, use

PAGE_HANDLE_WAIT = _asyncio.Event()
safe_end = threading.Event()

DATA = 0x00
SIGNAL = 0x01


class WebSocketRegistry:
    SOCK_TYPE_DATA = DATA
    SOCK_TYPE_SIGNAL = SIGNAL
    connections: list[Optional[websockets.WebSocketServerProtocol]] = [None, None]
    message_queue = asyncio.Queue()
    connections_completed = _asyncio.Event()

    @classmethod
    def get_websocket(cls, type_id: Union[DATA, SIGNAL]):
        return cls.connections[type_id]

    @classmethod
    def set_websocket(cls, type_id: Union[DATA, SIGNAL], websocket):
        cls.connections[type_id] = websocket

    @classmethod
    def send_data(cls, data, type_of_data):
        cls.message_queue.put((data, type_of_data))

    @classmethod
    async def start_data_sender(cls):
        while True:
            data_packet, data_type = await cls.message_queue.get()
            if safe_end.is_set():
                return
            await cls.get_websocket(data_type).send(data_packet)

    @classmethod
    def clear(cls):
        cls.message_queue.put(None)
        for i in cls.connections:
            if i is not None:
                i.close()
        del cls.connections


class ReplyRegistry:
    messages_to_futures_mapping: dict[str, _asyncio.Future] = {}
    id_factory = itertools.count()

    @classmethod
    def register_reply(cls, data: DataWeaver):
        data.id = next(cls.id_factory)
        fut = asyncio.get_event_loop().create_future()
        cls.messages_to_futures_mapping[data.id] = fut
        return fut

    @classmethod
    def reply_arrived(cls, data: DataWeaver):
        if data.id in cls.messages_to_futures_mapping:
            fut = cls.messages_to_futures_mapping[data.id]
            fut.set_result(data)


def get_verified_type(data: DataWeaver, web_socket):
    if data.match_header(WebSocketRegistry.SOCK_TYPE_DATA):
        print("page data connected")  # debug
        WebSocketRegistry.set_websocket(SIGNAL, web_socket)
        return importlib.import_module('src.core.webpage_handlers.handledata').handler
    if data.match_header(WebSocketRegistry.SOCK_TYPE_SIGNAL):
        print("page signals connected")  # debug
        WebSocketRegistry.set_websocket(DATA, web_socket)
        return importlib.import_module('src.core.webpage_handlers.handlesignals').handler


async def handle_client(web_socket: websockets.WebSocketServerProtocol):
    wire_data = await web_socket.recv()
    verification = DataWeaver(serial_data=wire_data)
    handle_function = get_verified_type(verification, web_socket)
    try:
        print("waiting for data", use.func_str(handle_function))
        if handle_function:
            async for data in web_socket:
                use.echo_print("data from page:", data, '\a')
                use.echo_print(f"forwarding to {use.func_str(handle_function)}")
                parsed_data = DataWeaver(serial_data=data)
                if parsed_data.is_reply:
                    ReplyRegistry.reply_arrived(parsed_data)
                _asyncio.create_task(handle_function(parsed_data))
                if safe_end.is_set():
                    return
        else:
            print("Unknown connection type")
            await web_socket.close()
    except websockets.exceptions.ConnectionClosed:
        print("Websocket Connection closed")


async def start_websocket_server():
    start_server = await websockets.serve(handle_client, const.THIS_IP, const.PORT_PAGE)
    use.echo_print(f"websocket server started at ws://{const.THIS_IP}:{const.PORT_PAGE}")
    async with start_server:
        await PAGE_HANDLE_WAIT.wait()


async def initiate_pagehandle():
    asyncio.create_task(WebSocketRegistry.start_data_sender())
    await start_websocket_server()


def end():
    global PAGE_HANDLE_WAIT
    PAGE_HANDLE_WAIT.set()
    safe_end.set()
    WebSocketRegistry.clear()


async def dispatch_data(data: DataWeaver, expect_reply=False):
    if check_closing():
        return
    print(f"::Sending data to page: {data}")
    if expect_reply:
        await ReplyRegistry.register_reply(data)
    WebSocketRegistry.send_data(data, data.type)


def new_message_arrived(message_data: WireData):
    d = DataWeaver(
        header=message_data.header,
        content=message_data['message'],
        _id=message_data.id,
    )
    WebSocketRegistry.send_data(d, DATA)


def check_closing():
    if safe_end.is_set():
        use.echo_print("Can't send data to page, safe_end is flip", safe_end)
        return True
    return False
