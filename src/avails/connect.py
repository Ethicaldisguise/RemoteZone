import asyncio as _asyncio
import logging
import socket
import socket as _socket
import struct
from abc import ABC, abstractmethod
from typing import IO, Optional, Self

from src.avails import const, useables

_logger = logging.getLogger(__name__)


class Socket(_socket.socket):
    """
    The Socket class is a custom subclass of Python's built-in _socket.socket class,
    designed to integrate with asyncio for asynchronous I/O operations.
    It provides both synchronous and asynchronous methods for common socket operations.

    Attributes:
        __loop (asyncio.AbstractEventLoop): The event loop used for asynchronous operations.
         This must be set using the set_loop method before any asynchronous methods are called.
    """

    __loop: Optional[_asyncio.AbstractEventLoop] = None

    # def __init__(self, family: _socket.AddressFamily | int = -1, _type: _socket.SocketKind | int = -1, proto: int = -1, fileno: int | None = None) -> None:
    #     super().__init__(family, _type, proto, fileno)

    def set_loop(self, loop: _asyncio.AbstractEventLoop):
        """
        Sets the event loop to be used for asynchronous operations.

        Parameters:
            loop (asyncio.AbstractEventLoop): The event loop instance.
        """
        self.__loop = loop

    def remove_loop(self):
        self.__loop = None

    def accept(self) -> tuple[Self, tuple[str, int]]:
        """
        Accepts a connection, returning a new custom Socket instance and the address of the client.
        Returns:
            custom_socket (Socket): A new Socket instance.
            addr (Tuple[str, int]): The address of the client.
        """
        fd, addr = self._accept()  # noqa
        # Cast the socket to the custom Socket class

        custom_socket = self.__class__(self.family, self.type, self.proto, fileno=fd)
        if _socket.getdefaulttimeout() is None and self.gettimeout():
            custom_socket.setblocking(True)
        if self.__loop:
            custom_socket.set_loop(self.__loop)
        return custom_socket, addr

    async def aaccept(self):
        return await self.__loop.sock_accept(self)

    def arecv(self, bufsize):
        return self.__loop.sock_recv(self, bufsize)

    async def aconnect(self, __address):
        return await self.__loop.sock_connect(self, __address)

    async def asendall(self, data):
        return await self.__loop.sock_sendall(self, data)

    async def asendfile(self, file: IO[bytes],
                        offset: int = 0,
                        count: int | None = None,
                        *,
                        fallback: bool | None = None):
        return await self.__loop.sock_sendfile(self, file, offset, count, fallback=fallback)

    async def asendto(self, data, address):
        return await self.__loop.sock_sendto(self, data, address)

    async def arecv_into(self, buffer):
        return await self.__loop.sock_recv_into(self, buffer)

    async def arecvfrom_into(self, buffsize):
        return await self.__loop.sock_recvfrom(self, buffsize)

    async def arecvfrom(self, buffsize):
        return await self.__loop.sock_recvfrom(self, buffsize)


class NetworkProtocol(ABC):
    __slots__ = ()

    @staticmethod
    @abstractmethod
    def create_async_sock(loop: _asyncio.AbstractEventLoop,
                          family: _socket.AddressFamily,
                          fileno: int = None):
        """Create an asynchronous socket."""
        return NotImplemented

    @staticmethod
    @abstractmethod
    def create_sync_sock(family: _socket.AddressFamily = _socket.AF_INET,
                         fileno: int = None):
        """Create a synchronous socket."""
        return NotImplemented

    @staticmethod
    @abstractmethod
    def create_async_server_sock(loop: _asyncio.AbstractEventLoop,
                                 bind_address, *,
                                 family: _socket.AddressFamily = _socket.AF_INET,
                                 backlog: int | None = None,
                                 fileno: int = None):
        """Create an asynchronous server socket."""
        return NotImplemented

    @staticmethod
    @abstractmethod
    def create_sync_server_sock(bind_address, *,
                                family: _socket.AddressFamily = _socket.AF_INET,
                                backlog: int | None = None,
                                fileno: int = None):
        """Create a synchronous server socket."""
        return NotImplemented

    @staticmethod
    @abstractmethod
    async def create_connection_async(loop, address, timeout=None) -> Socket:
        return NotImplemented

    def __format__(self, format_spec):
        return self.__repr__().__format__(format_spec)


class TCPProtocol(NetworkProtocol):
    __slots__ = ()

    @staticmethod
    def create_async_sock(loop: _asyncio.AbstractEventLoop,
                          family: _socket.AddressFamily = _socket.AF_INET,
                          fileno: Optional[int] = None) -> Socket:
        sock = Socket(family, _socket.SOCK_STREAM, -1, fileno)
        sock.setblocking(False)
        sock.set_loop(loop)
        return sock

    @staticmethod
    def create_sync_sock(family: _socket.AddressFamily = _socket.AF_INET,
                         fileno: Optional[int] = None):
        return Socket(family, _socket.SOCK_STREAM, -1, fileno)

    @staticmethod
    def create_async_server_sock(loop: _asyncio.AbstractEventLoop,
                                 bind_address, *,
                                 family: _socket.AddressFamily = _socket.AF_INET,
                                 backlog: int | None = None,
                                 fileno: Optional[int] = None):
        server_sock = Socket(family, _socket.SOCK_STREAM, -1, fileno)
        server_sock.bind(bind_address)
        server_sock.setblocking(False)
        server_sock.set_loop(loop)
        server_sock.listen(backlog)
        return server_sock

    @staticmethod
    def create_sync_server_sock(bind_address, *,
                                family: _socket.AddressFamily = _socket.AF_INET,
                                backlog: int | None = None,
                                fileno: Optional[int] = None):
        server_sock = Socket(family, _socket.SOCK_STREAM, -1, fileno)
        server_sock.bind(bind_address)
        server_sock.listen(backlog)
        return server_sock

    @classmethod
    async def create_connection_async(cls, loop: _asyncio.AbstractEventLoop, address, timeout=None) -> Socket:
        try:
            addr_info = await loop.getaddrinfo(*address, type=_socket.SOCK_STREAM)
            addr_family, sock_type, _, _, address = addr_info[0]
        except TypeError as tpe:
            _logger.error(f"something wrong with the given address: {address}", exc_info=tpe)
            raise
        sock = cls.create_async_sock(loop, addr_family)
        await (_asyncio.wait_for(sock.aconnect(address), timeout) if timeout else sock.aconnect(address))
        return sock

    def __repr__(self):
        return "<connect.TCPProtocol>"

    def __format__(self, format_spec):
        return self.__repr__().__format__(format_spec)


class UDPProtocol(NetworkProtocol):
    __slots__ = ()

    @staticmethod
    def create_async_sock(loop: _asyncio.AbstractEventLoop,
                          family: _socket.AddressFamily = _socket.AF_INET,
                          fileno: Optional[int] = None):
        sock = Socket(family, _socket.SOCK_DGRAM, _socket.IPPROTO_UDP, fileno)
        sock.setblocking(False)
        sock.set_loop(loop)
        return sock

    @staticmethod
    def create_sync_sock(family: _socket.AddressFamily = _socket.AF_INET,
                         fileno: Optional[int] = None):
        return Socket(family, _socket.SOCK_DGRAM, -1, fileno)

    @staticmethod
    def create_async_server_sock(loop: _asyncio.AbstractEventLoop,
                                 bind_address, *,
                                 family: _socket.AddressFamily = _socket.AF_INET,
                                 backlog: int | None = None,
                                 fileno: Optional[int] = None):
        server_sock = Socket(family, _socket.SOCK_DGRAM, -1, fileno)
        server_sock.setblocking(False)
        server_sock.set_loop(loop)
        server_sock.bind(bind_address)
        # server_sock.listen(backlog)
        return server_sock

    @staticmethod
    def create_sync_server_sock(bind_address, *,
                                family: _socket.AddressFamily = _socket.AF_INET,
                                backlog: int | None = None,
                                fileno: Optional[int] = None):
        server_sock = Socket(family, _socket.SOCK_DGRAM, -1, fileno)
        server_sock.bind(bind_address)
        server_sock.listen(backlog)
        return server_sock

    @classmethod
    async def create_connection_async(cls, loop: _asyncio.AbstractEventLoop, address, timeout=None) -> Socket:
        addr_info = await loop.getaddrinfo(*address, type=_socket.SOCK_DGRAM)
        addr_family, sock_type, _, _, address = addr_info[0]

        sock = cls.create_async_sock(loop, addr_family)
        sock.connect(address)
        return sock

    def __repr__(self):
        return "<connect.UDProtocol>"


class _PauseMixIn:
    __slots__ = ()

    def pause(self):
        self._stopper.clear()  # noqa


class _ResumeMixIn:
    __slots__ = ()

    def resume(self):
        self._stopper.set()  # noqa


class Sender(_PauseMixIn, _ResumeMixIn):
    __slots__ = 'sock', 'send_func', '_stopper'

    def __init__(self, sock):
        self.sock = sock
        self.send_func = _asyncio.get_event_loop().sock_sendall
        self._stopper = _asyncio.Event()
        self._stopper.set()

    async def __call__(self, buf: bytes):
        await self._stopper.wait()
        return await self.send_func(self.sock, buf)


class Receiver(_PauseMixIn, _ResumeMixIn):
    __slots__ = 'sock', 'recv_func', '_stopper'

    def __init__(self, sock):
        self.sock = sock
        self.recv_func = _asyncio.get_event_loop().sock_recv
        self._stopper = _asyncio.Event()
        self._stopper.set()

    async def __call__(self, nbytes: int):
        await self._stopper.wait()
        return await self.recv_func(self.sock, nbytes)


def create_connection_sync(address, addr_family=None, sock_type=None, timeout=None) -> Socket:
    try:
        if addr_family is None:
            addr_family = const.IP_VERSION
        if sock_type is None:
            sock_type = _socket.SOCK_STREAM
        addr_info = _socket.getaddrinfo(*address, family=addr_family, type=sock_type)
    except TypeError as tpe:
        _logger.info("something wrong with the given address ", exc_info=tpe)
        raise

    addr_family, sock_type, _, _, address = addr_info[0]
    sock = Socket(addr_family, sock_type)
    sock.settimeout(timeout)
    sock.connect(address)
    return sock


async def create_connection_async(address, timeout=None) -> Socket:
    loop = _asyncio.get_running_loop()
    sock = await const.PROTOCOL.create_connection_async(loop, address, timeout)
    return sock


CONN_URI = 'uri'
REQ_URI = 'req_uri'


def connect_to_peer(_peer_obj=None, to_which: str = CONN_URI, timeout=0.001, retries: int = 1) -> Socket:
    """
    Creates a basic socket connection to the peer_obj passed in.
    pass `const.REQ_URI_CONNECT` to connect to req_uri of peer
    :param timeout: self-explanatory
    :param to_which: specifies to what uri should the connection made
    :param _peer_obj: RemotePeer object
    :param retries: if given tries reconnecting with exponential backoff using :func:`useables.get_timeouts`
            uses :param timeout: as initial value
    :return:
    """
    address, sock_family, sock_type = resolve_address(_peer_obj, to_which)
    retry_count = 0
    for timeout in useables.get_timeouts(timeout, max_retries=retries):
        try:
            return create_connection_sync(address, addr_family=sock_family, sock_type=sock_type, timeout=timeout)
        except (OSError, _socket.error):
            retry_count += 1
            if retry_count >= retries:
                raise


def resolve_address(_peer_obj, to_which):
    addr = getattr(_peer_obj, to_which)
    address_info = _socket.getaddrinfo(addr[0], addr[1], type=_socket.SOCK_STREAM)[0]
    address = address_info[4]  # [:2]
    sock_family = address_info[0]
    sock_type = address_info[1]
    return address, sock_family, sock_type


@useables.awaitable(resolve_address)
async def resolve_address(_peer_obj, to_which):
    addr = getattr(_peer_obj, to_which)
    loop = _asyncio.get_running_loop()
    address_info, *_ = await loop.getaddrinfo(addr[0], addr[1], type=_socket.SOCK_STREAM)
    address = address_info[4]  # [:2]
    sock_family = address_info[0]
    sock_type = address_info[1]
    return address, sock_family, sock_type


@useables.awaitable(connect_to_peer)
async def connect_to_peer(_peer_obj=None, to_which: int = CONN_URI, timeout=None, retries: int = 1) -> Socket:
    """
    Creates a basic socket connection to the peer_obj passed in.
    pass `const.REQ_URI_CONNECT` to connect to req_uri of peer
    *args will be passed into socket.setsockopt
    :param timeout: self-explanatory, default is None
    :param to_which: specifies to what uri should the connection made
    :param _peer_obj: RemotePeer object
    :param retries: if given tries reconnecting with exponential backoff using :func:`useables.get_timeouts`
    :param timeout: uses as initial value
    :returns: connected socket if successful
    :raises: OSError
    """

    address, sock_family, sock_type = await resolve_address(_peer_obj, to_which)
    address = address[:2]
    retry_count = 0

    if timeout is None:
        return await create_connection_async(address)

    for timeout in useables.get_timeouts(timeout, max_retries=retries):
        try:
            return await create_connection_async(address, timeout)
        except OSError:
            retry_count += 1
            if retry_count >= retries:
                raise


def is_socket_connected(sock: Socket):
    blocking = sock.getblocking()
    keep_alive = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_KEEPALIVE)

    try:
        sock.setblocking(False)
        sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_KEEPALIVE, 1)
        sock.getpeername()
        data = sock.recv(1, _socket.MSG_PEEK)
        return data != b''
    except BlockingIOError:
        return True
    except (OSError, _socket.error):
        return False
    finally:
        try:
            sock.setblocking(blocking)
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_KEEPALIVE, keep_alive)
        except OSError:
            return False


def get_free_port(ip=None) -> int:
    """Gets a free port from the system."""
    ip = ip or str(const.THIS_IP)

    with _socket.socket(const.IP_VERSION, _socket.SOCK_STREAM) as s:
        s.bind((ip, 0))  # Bind to port 0 to get a free port
        return s.getsockname()[1]  # Return the chosen port


def is_port_empty(port, addr=None):
    addr_info = _socket.getaddrinfo(str(addr), port, type=_socket.SOCK_STREAM)[0]
    family, sock_type, proto, _, sock_addr = addr_info
    with _socket.socket(family, sock_type, proto) as s:
        try:
            # Try to bind the socket to the specified port
            s.bind(sock_addr)
            return True  # Port is empty
        except OSError:
            return False  # Port is not empty or some other error occurred


def ipv4_multicast_socket_helper(
        sock,
        multicast_addr,
        *,
        loop_back=0,
        reuse_addr=1,
        ttl=1,
        add_membership=True
):
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, loop_back)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, reuse_addr)
    if add_membership:
        group = socket.inet_aton(f"{multicast_addr[0]}")
        mreq = struct.pack('4sl', group, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock_options = {
        'membership': add_membership,
        'loop_back': loop_back,
        'ttl': ttl,
        'reuse_addr': reuse_addr
    }
    _logger.debug(f"socket{sock} options for multicast {sock_options}")


def ipv6_multicast_socket_helper(
        sock, multicast_addr,
        *,
        loop_back=1,
        reuse_addr=1,
        add_membership=True,
        hops=1
):
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_LOOP, loop_back)
    # this function makes socket to go with default interface
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, reuse_addr)
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, hops)
    group = socket.inet_pton(socket.AF_INET6, f"{multicast_addr[0]}")
    if add_membership:
        mreq = group + struct.pack('@I', 0)  # default interface
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)
    sock_options = {
        'membership': add_membership,
        'loop_back': loop_back,
        'hops': hops,
        'reuse_addr': reuse_addr
    }
    _logger.debug(f"socket={sock} options for multicast {sock_options}")
