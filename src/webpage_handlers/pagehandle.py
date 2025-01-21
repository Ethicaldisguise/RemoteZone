import asyncio as _asyncio
import importlib
import itertools
from typing import TYPE_CHECKING, Union

import websockets

from src.avails import DataWeaver, InvalidPacket, StatusMessage, const, use
from src.avails.events import StreamDataEvent
from src.avails.exceptions import UnknownConnectionType, WebSocketRegistryReStarted
from src.avails.useables import wrap_with_tryexcept
from src.core import Dock
from src.transfers import HEADERS
from src.webpage_handlers import logger

if TYPE_CHECKING:
    from websockets.asyncio.connection import Connection
else:
    Connection = None

safe_end = Dock.finalizing
PROFILE_WAIT = _asyncio.Event()


class WebSocketRegistry:
    # :todo: make WebSocketRegistry compatible with QueueMixIn and attach with it
    connections: dict[int, Connection] = {}
    message_queue = _asyncio.Queue()
    connections_completed = _asyncio.Event()
    _start_data_sender_task_reference = None
    _started = False

    @classmethod
    def get_websocket(cls, type_id: Union[const.DATA, const.SIGNAL]):
        return cls.connections[type_id]

    @classmethod
    def set_websocket(cls, type_id: Union[const.DATA, const.SIGNAL], websocket):
        cls.connections[type_id] = websocket

    @classmethod
    def send_data(cls, data, type_of_data):
        """
        does not actually send data, adds to a message queue
        """
        cls.message_queue.put_nowait((data, type_of_data))

    @classmethod
    async def start_data_sender(cls):
        if cls._started:
            raise WebSocketRegistryReStarted("reentered registry")
        cls._started = True
        while True:
            data_packet, data_type = await cls.message_queue.get()
            if safe_end.is_set():
                break
            web_socket = cls.get_websocket(data_type)
            await web_socket.send(
                str(data_packet)  # make sure that data is a string
            )
        cls._started = False

    @classmethod
    async def _clear(cls):
        cls.message_queue.put_nowait((None, None))
        for i in cls.connections.values():
            await i.close()
        del cls.connections

    @classmethod
    async def __aenter__(cls):
        f = wrap_with_tryexcept(cls.start_data_sender)
        cls._start_data_sender_task_reference = _asyncio.create_task(f())
        return cls

    @classmethod
    async def __aexit__(cls, exc_type, exc_val, exc_tb):
        await cls._clear()
        cls._start_data_sender_task_reference.cancel()


class ReplyRegistry:
    messages_to_futures_mapping: dict[str, _asyncio.Future] = {}
    id_factory = itertools.count()

    @classmethod
    def register_reply(cls, data: DataWeaver) -> _asyncio.Future[DataWeaver]:
        loop = _asyncio.get_event_loop()
        data.msg_id = next(cls.id_factory)
        cls.messages_to_futures_mapping[data.msg_id] = fut = loop.create_future()
        return fut

    @classmethod
    def reply_arrived(cls, data: DataWeaver):
        if data.msg_id in cls.messages_to_futures_mapping:
            fut = cls.messages_to_futures_mapping[data.msg_id]
            fut.set_result(data)

    @classmethod
    def is_registered(cls, data: DataWeaver):
        return data.msg_id in cls.messages_to_futures_mapping


def _get_verified_type(data: DataWeaver, web_socket):
    WebSocketRegistry.set_websocket(data.header, web_socket)
    if data.match_header(const.DATA):
        logger.debug("[PAGE HANDLE] page data connected")  # debug
        return importlib.import_module("src.core.webpage_handlers.handledata").handler
    if data.match_header(const.SIGNAL):
        logger.debug("[PAGE HANDLE] page signals connected")  # debug
        return importlib.import_module(
            "src.core.webpage_handlers.handlesignals"
        ).handler


async def validate_connection(web_socket):
    try:
        wire_data = await _asyncio.wait_for(web_socket.recv(), const.SERVER_TIMEOUT)
    except TimeoutError:
        logger.error(f"[PAGE HANDLE] timeout reached, cancelling {web_socket=}")
        await web_socket.close()
        raise ConnectionError() from None

    verification = DataWeaver(serial_data=wire_data)
    handle_function = _get_verified_type(verification, web_socket)
    logger.info("[PAGE HANDLE] waiting for data from websocket")

    if not handle_function:
        logger.info("[PAGE HANDLE] Unknown connection type, closing websocket")
        await web_socket.close()
        raise UnknownConnectionType()
    return handle_function


async def handle_client(web_socket: Connection):
    try:
        try:
            handle_function = await validate_connection(web_socket)
        except (ConnectionError, UnknownConnectionType):
            return

        async for data in web_socket:
            logger.info(f"[PAGE HANDLE] data from page: {data=}")
            parsed_data = DataWeaver(serial_data=data)

            try:
                parsed_data.field_check()
            except InvalidPacket as ip:
                logger.debug("[PAGE HANDLE]", exc_info=ip)
                return

            if ReplyRegistry.is_registered(parsed_data):
                ReplyRegistry.reply_arrived(parsed_data)
                continue

            f = use.wrap_with_tryexcept(handle_function, parsed_data)
            _asyncio.create_task(f())
            if safe_end.is_set():
                return

    except websockets.exceptions.ConnectionClosed:
        logger.info("[PAGE HANDLE] Websocket Connection closed")


async def start_websocket_server():
    start_server = await websockets.serve(handle_client, const.WEBSOCKET_BIND_IP, const.PORT_PAGE)
    logger.info(f"[PAGE HANDLE] websocket server started at ws://{const.WEBSOCKET_BIND_IP}:{const.PORT_PAGE}")
    async with start_server:
        await Dock.finalizing.wait()

    logger.info("[PAGE HANDLE] ending websocket server")


async def initiate_page_handle():
    async with WebSocketRegistry():
        await start_websocket_server()


def dispatch_data(data, expect_reply=False):
    """Send data to frontend

    Not actually sends data but queues the :param data: for sending

    Args:
        data (DataWeaver) : data to send
        expect_reply (bool) : if this argument is true then a `asyncio.Future` is returned which can be awaited
            overrides any msg_id attached to data (DataWeaver)
    Returns:
        bool | asyncio.Future[DataWeaver]
    """

    def check_closing():
        if safe_end.is_set():
            logger.debug("[PAGE HANDLE] Can't send data to page, safe_end is flip")
            return True
        return False

    if check_closing():
        return False

    logger.info(f"[PAGE HANDLE] ::Sending data to page: {data!r}")
    WebSocketRegistry.send_data(data, data.type)
    if expect_reply:
        return ReplyRegistry.register_reply(data)
    return True


def send_status_data(data: StatusMessage):
    logger.info(f"[PAGE HANDLE] ::Status update to page: {data!r}")
    WebSocketRegistry.send_data(data, const.SIGNAL)


def MessageHandler():
    async def handler(event: StreamDataEvent):
        d = DataWeaver(
            header=event.data.header,
            content=event.data["message"],
            peer_id=event.data.peer_id,
        )
        WebSocketRegistry.send_data(d, const.DATA)

    return handler


def register_handler_to_acceptor(acceptor_disp):
    return acceptor_disp.register_handler(
        HEADERS.CMD_TEXT,
        MessageHandler()
    )
