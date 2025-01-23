import asyncio
import functools
import inspect
import os
import platform
import socket
import struct
import subprocess
import sys
import threading
import traceback
import typing
from pathlib import Path
from socket import AddressFamily, IPPROTO_TCP, IPPROTO_UDP
from sys import _getframe  # noqa
from typing import Annotated, Awaitable, Final
from uuid import uuid4

import select

from src.avails import const


def func_str(func_name):
    return f"{func_name.__name__}()\\{os.path.relpath(func_name.__code__.co_filename)}"


def get_unique_id(_type: type = str):
    if _type == bytes:
        return uuid4().bytes
    return _type(uuid4())


SHORT_INT = 4
LONG_INT = 8


def shorten_path(path: Path, max_length):
    if len(str(path)) <= max_length:
        return str(path)
    selected_parts = list(path.parts)
    part_ptr = 1
    while len("".join(selected_parts)) >= max_length:
        selected_parts.pop(part_ptr)
    selected_parts.insert(1, '..')
    return os.path.sep.join(selected_parts)


async def recv_int(get_bytes: typing.Callable[[int], Awaitable[bytes]], type=SHORT_INT):
    """Receives integer from get_bytes function

    awaits on ``get_bytes``, gets bytes based on ``type`` argument
    unpacks it using ``struct.unpack``

    Args:
        get_bytes (Callable[[int], Awaitable[bytes]]): function to receive bytes from
        type: `SHORT_INT` and `LONG_INT`

    Returns:
        int: unpacked integer

    Raises:
        ValueError : on ConnectionResetError or struct.error
    """
    try:
        byted_int = await get_bytes(type)
        integer = struct.unpack('!I' if type == SHORT_INT else '!Q', byted_int)[0]
        return integer
    except struct.error as se:
        raise ValueError(f"unable to unpack integer from: {byted_int}") from se
    except ConnectionResetError as ce:
        raise ValueError(f"unable to receive integer") from ce


def get_timeouts(initial=0.001, factor=2, max_retries=const.MAX_RETIRES, max_value=5.0):
    """
    Generate exponential backoff timeout values.

    Args:
        initial (float): The initial timeout value in seconds. Defaults to 0.001.
        factor (int): The factor by which the timeout value is multiplied at each step. Defaults to 2.
        max_retries (int): The maximum number of retries. Defaults to 5, if -1 is provided then yields infinitely
        max_value (float): The maximum timeout value in seconds. Defaults to 5.0.

    Yields:
        float: The next timeout value in the sequence, capped by max_value.

    Example:
        >>> list(get_timeouts())
        [0.001, 0.002, 0.004, 0.008, 0.016]

        >>> list(get_timeouts(initial=1, factor=3, max_retries=4, max_value=10))
        [1, 3, 9, 10]
    """
    current = initial

    if max_retries == -1:
        while True:
            if current >= max_value:
                yield max_value
            current *= factor

    for _ in range(max_retries):
        yield min(current, max_value)
        current *= factor


async def async_timeouts(*, initial=0.001, factor=2, max_retries=const.MAX_RETIRES, max_value=5.0):
    """
    same as :func: `get_timeouts` but delays itself in yielding

    Example:
        >>> async for _ in async_timeouts(initial=1, factor=2, max_retries=4, max_value=5):
        >>>     # any working code that needs to be executed with delays
    """

    for timeout in get_timeouts(initial,factor, max_retries, max_value):
        await asyncio.sleep(timeout)
        yield


def echo_print(*args, **kwargs):
    """Prints the given arguments to the console.

    Args:
        *args: The arguments to print.
    """
    # with LOCK_PRINT:
    return print(*args, COLOR_RESET, **kwargs)


def async_input(helper_str=""):
    return asyncio.get_event_loop().run_in_executor(None, input, helper_str)


def open_file(content):
    if platform.system() == "Windows":
        powershell_script = f"""
        $file = "{content}"
        Invoke-Item $file
        """
        result = subprocess.run(["powershell.exe", "-Command", powershell_script], stdout=subprocess.PIPE, text=True)
        return result.stdout.strip()
    elif platform.system() == "Darwin":
        subprocess.run(["open", content])
    else:
        subprocess.run(["xdg-open", content])
    return None


def wait_for_sock_read(sock, actuator, timeout):
    reads, _, _ = select.select([sock, actuator], [], [], timeout)

    if actuator.to_stop:
        return (actuator,)

    return reads


def wait_for_sock_write(sock, actuator, timeout):
    _, writes, _ = select.select([actuator, ], [sock, ], [], timeout)

    if actuator.to_stop:
        return [actuator, ]

    return writes


def send_data_with_retries(sock: socket.socket, data: bytes, chunk_size, retries):
    b = sock.getblocking()
    mm_data = memoryview(data)
    stop = False
    sock.setblocking(False)
    send_func = sock.send
    for _ in get_timeouts(max_retries=retries):
        i = 0
        while i < len(mm_data):
            sent = send_func(mm_data[i: i + chunk_size])
            i += sent


_CO_NESTED = inspect.CO_NESTED
_CO_FROM_COROUTINE = inspect.CO_COROUTINE | inspect.CO_ITERABLE_COROUTINE | inspect.CO_ASYNC_GENERATOR


def from_coroutine(level=2, _cache={}):  # noqa
    f_code = _getframe(level).f_code
    if f_code in _cache:
        return _cache[f_code]
    if f_code.co_flags & _CO_FROM_COROUTINE:
        _cache[f_code] = True
        return True
    else:
        if f_code.co_flags & _CO_NESTED and f_code.co_name[0] == '<':
            return from_coroutine(level + 2)
        else:
            _cache[f_code] = False
            return False


def awaitable(syncfunc):
    """
    # this uses code from curio package

    Author : Dabeaz
    Repo : https://github.com/dabeaz/curio

    Decorator that allows an asynchronous function to be paired with a
    synchronous function in a single function call.  The selection of
    which function executes depends on the calling context.  For example:

        def spam(sock, maxbytes):                       (A)
            return sock.recv(maxbytes)

        @awaitable(spam)                                (B)
        async def spam(sock, maxbytes):
            return await sock.recv(maxbytes)

    In later code, you could use the spam() function in either a synchronous
    or asynchronous context.  For example:

        def foo():
            ...
            r = spam(s, 1024)          # Calls synchronous function (A) above
            ...

        async def bar():
            ...
            r = await spam(s, 1024)    # Calls async function (B) above
            ...

    """

    def decorate(asyncfunc):
        @functools.wraps(asyncfunc)
        def wrapper(*args, **kwargs):
            if from_coroutine():
                return asyncfunc(*args, **kwargs)
            else:
                return syncfunc(*args, **kwargs)

        wrapper._syncfunc = syncfunc
        wrapper._asyncfunc = asyncfunc
        wrapper._awaitable = True
        wrapper.__doc__ = syncfunc.__doc__ or asyncfunc.__doc__
        return wrapper

    return decorate


COLORS: Final[list[str]] = [
    "\033[91m",  # Red
    "\033[92m",  # Green
    "\033[93m",  # Yellow
    "\033[94m",  # Blue
    "\033[95m",  # Magenta
]

COLOR_RESET: Final[str] = "\033[0m"


def wrap_with_tryexcept(func, *args, **kwargs):
    """
    Designed to use like:

    >>> f = wrap_with_tryexcept(func, *args, **kwargs)
    >>> asyncio.create_task(f())  # sort of `functools.partial` aesthetics

    Args:
        func : any async function
        args, kwargs : to forward

    """

    @functools.wraps(func)
    async def wrapped_with_tryexcept():
        try:
            nonlocal args, kwargs
            # print("wrapping with try except", func_str(func))
            return await func(*args, **kwargs)
        except Exception as e:

            print(f"{COLORS[1]}got an exception for function {func_str(func)} : {type(e)} : {e}", file=sys.stderr)
            traceback.print_exc()
            tb = traceback.extract_tb(e.__traceback__)
            filtered_tb = [frame for frame in tb if "wrapped_with_tryexcept" not in frame.name]

            print(
                f"{COLORS[1]}got an exception for function {func_str(func)} : {type(e).__name__} : {e}",
                file=sys.stderr,
            )
            # Print the filtered traceback
            for frame in filtered_tb:
                print(f"  File \"{frame.filename}\", line {frame.lineno}, in {frame.name}")
                if frame.line:
                    print(f"    {frame.line}")
            print(COLOR_RESET)
            raise

    return wrapped_with_tryexcept


def spawn_task(func, *args, bookeep=None, done_callback=None, **kwargs):
    f = wrap_with_tryexcept(func, *args, **kwargs)
    t = asyncio.create_task(f())
    if done_callback:
        t.add_done_callback(done_callback)
    if bookeep:
        bookeep(t)
    else:
        return t


def search_relevant_peers(peer_list, search_string) -> list:
    """
    Searches for relevant peers based on the search string,

    Uses a copy of the current peer IDs to avoid modification errors.
    Provides an option to use a lock for safety if the GIL is not guaranteed
    (e.g., Jython).

    Args:
        search_string (str): The string to search for relevance.
        peer_list(PeerDict): The dictionary of id to peer object mapping
    Returns:
        yields peers
    """
    if not hasattr(peer_list, '_gil_safe'):  # Check if GIL safety has been determined
        peer_list._gil_safe = hasattr(threading, 'get_ident')  # Check for GIL

    peer_ids = list(peer_list.keys())

    for peer_id in peer_ids:
        try:
            peer = peer_list[peer_id]  # May raise KeyError if removed concurrently
        except KeyError:
            continue  # Skip removed peer
        if peer.is_relevant(search_string):
            yield peer


_AddressFamily = Annotated[AddressFamily, 'v4 or v6 family']
_SockType = Annotated[socket.SOCK_STREAM | socket.SOCK_DGRAM, 'STREAM OR UDP']
_IpProto = Annotated[IPPROTO_TCP | IPPROTO_UDP, "tcp or udp protocol"]
_CannonName = Annotated[str, 'cannonical name']
_SockAddr = Annotated[tuple[str, int] | tuple[str, int, int, int], "address tuple[2] if v4 tuple[4] if v6"]


async def get_addr_info(
        host: bytes | str | None,
        port: bytes | str | int | None,
        *,
        family: int = 0,
        type: int = 0,  # noqa
        proto: int = 0,
        flags: int = 0
):
    """Just a convenience for asynchronous name resolving

    Args:
        host: passed into get addr info
        port: passed into get addr info
        family: passed into get addr info
        type: passed into get addr info
        proto: passed into get addr info
        flags: passed into get addr info

    Yields:
        tuple[
            _AddressFamily,
            _SockType,
            _IpProto,
            _CannonName,
            _SockAddr,
        ]
    """

    loop = asyncio.get_running_loop()
    addresses = await loop.getaddrinfo(host, port, family=family, type=type,
                                       proto=proto, flags=flags)

    for family, sock_type, proto, canonname, addr in addresses:
        yield family, sock_type, proto, canonname, addr


class NotInUse:
    __annotations__ = {
        'function': str,
        '__doc__': str
    }
    __slots__ = 'function', '__doc__'

    def __init__(self, function):
        """Decorator class to mark functions as not in use or not fully tested.

        This class is used to mark functions that are not currently in use or haven't been fully tested.
        By marking a function with this class, it prevents the call to the function unless explicitly allowed by the user.

        Args:
            function: The function to be decorated.

        Raises:
            ValueError : if function gets called
        """
        self.function = function

    def __call__(self, *args, **kwargs):
        """
        Args:
        - *args: Positional arguments for the function.
        - **kwargs: Keyword arguments for the function.
        """
        raise ValueError(f"Your are not supposed to call this function :{self.function.__name__}")
