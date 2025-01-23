import asyncio
import contextlib
import logging
import socket
import struct
from contextlib import aclosing
from pathlib import Path

from src.avails import Wire, WireData, connect, const
from src.avails.exceptions import TransferIncomplete
from src.avails.status import StatusMixIn
from src.core import get_this_remote_peer
from src.transfers import HEADERS
from src.transfers._fileobject import FileItem, TransferState, recv_file_contents, \
    send_actual_file, validatename

_logger = logging.getLogger(__name__)

# :todo: move all the status mixin mechanisms to manager level API,


class Sender(StatusMixIn):
    version = const.VERSIONS['FO']
    """
        stopping_flag(Callable[[],bool]): this gets called to check whether to stop or not, while sending chunks
    
        yield_freq(int): number of times this function should yield while sending chunks

    """

    def __init__(self, file_list, peer_obj, transfer_id, *, status_yield_frequency=10):
        self.state = TransferState.PREPARING
        self.file_list = [
            FileItem(x, seeked=0) for x in file_list
        ]
        self.socket = None
        self._file_id = transfer_id
        self.peer_obj = peer_obj
        super().__init__(status_yield_frequency)
        self.to_stop = False
        self._current_index = 0
        self._log_prefix = f"FILE[{self._file_id}]"
        self.send_func = None
        self.recv_func = None

    async def send_files(self):
        _logger.debug(f'{self._log_prefix} changing state to sending')
        self.state = TransferState.SENDING

        for index in range(self._current_index, len(self.file_list)):
            # there is another file incoming
            if self.to_stop:
                break
            file_item = self.file_list[index]
            self.current_file = index

            try:
                # a signal that says there is more to receive
                await self.send_func(struct.pack('?', True))
                await self._send_file_item(file_item)
                self.status_setup(
                    prefix=f"sending: {file_item}",
                    initial_limit=file_item.seeked,
                    final_limit=file_item.size
                )
                async with aclosing(self._send_single_file(file_item)) as sender:
                    async for items in sender:
                        yield items
            except OSError:
                _logger.error(f"{self._log_prefix} got os error, pausing transfer", exc_info=True)
                self.state = TransferState.PAUSED
                raise

        # end of transfer, signalling that there are no more files
        await self.send_func(struct.pack('?', False))
        _logger.info(f"{self._log_prefix} sent final flag, completed sending")
        self.state = TransferState.COMPLETED

    async def _send_file_item(self, file_item):
        file_object = bytes(file_item)
        file_packet = struct.pack('!I', len(file_object)) + file_object
        await self.send_func(file_packet)

    async def _send_single_file(self, file_item):
        async with aclosing(
                send_actual_file(
                    self.send_func,
                    file_item,
                )
        ) as send_file:
            async for seeked in send_file:
                self.update_status(seeked)
                if self.to_stop:
                    break
                if self.should_yield():
                    yield file_item, seeked

        print("file sent", file_item)

    @contextlib.asynccontextmanager
    async def prepare_connection(self):
        _logger.debug(f"{self._log_prefix} changing state to connection")  # debug
        self.state = TransferState.CONNECTING
        try:
            with await connect.connect_to_peer(
                    self.peer_obj,
                    connect.CONN_URI,
                    timeout=2,
                    retries=2,
            ) as connection:
                connection.setsockopt(socket.SOL_SOCKET, socket.TCP_NODELAY, 1)
                await self._authorize_connection(connection)
                self.socket = connection
                self.send_func = connect.Sender(self.socket)
                self.recv_func = connect.Receiver(self.socket)

                _logger.debug(f"{self._log_prefix} connection established")
                yield
        except OSError as oops:
            if not self.state == TransferState.PAUSED:
                _logger.warning(f"{self._log_prefix} reverting state to PREPARING, failed to connect to peer",
                                exc_info=oops)
                self.state = TransferState.PREPARING
            raise

    async def _authorize_connection(self, connection):
        handshake = WireData(
            header=HEADERS.CMD_FILE_CONN,
            msg_id=get_this_remote_peer().peer_id,
            version=self.version,
            file_id=self._file_id,
        )
        await Wire.send_async(connection, bytes(handshake))
        _logger.debug("authorization header sent for file connection", extra={'id': self._file_id})

    async def continue_file_transfer(self):
        _logger.debug(f'FILE[{self._file_id}] changing state to sending')
        self.state = TransferState.SENDING
        start_file = self.file_list[self.current_file]
        self.to_stop = False

        # synchronizing last file sent
        seeked_int = await self.socket.recv(8)
        if not seeked_int:
            _logger.debug(f'FILE[{self._file_id}] changing state to paused')
            self.state = TransferState.PAUSED
            raise ConnectionResetError("Connection reset by other end while received file seek point")

        start_file.seeked = struct.unpack('!Q', seeked_int)[0]
        self.status_setup(f"resuming file:{start_file}", start_file.seeked, start_file.size)
        async with aclosing(self._send_single_file(start_file)) as initial_file_sender:
            async for items in initial_file_sender:
                yield items
        # end of broken file transfer

        # continuing with remaining transfer
        async with aclosing(self.send_files()) as file_sender:
            async for items in file_sender:
                yield items

    def attach_files(self, paths_list):
        self.file_list.extend(FileItem(Path(path), 0) for path in paths_list)

    @property
    def id(self):
        return self._file_id


class Receiver(StatusMixIn):
    version = const.VERSIONS['FO']

    def __init__(self, peer_id, file_id, download_path, *, yield_freq=10):
        self.state = TransferState.PREPARING
        self.peer_id = peer_id
        self._file_id = file_id
        self.connection_wait = asyncio.get_event_loop().create_future()
        self.connection = None
        self._log_prefix = f'FILE[{self.peer_id}]'
        self.download_path = download_path
        self.current_file = None
        self.to_stop = False
        self.file_items = []
        super().__init__(yield_freq)
        self.send_func = None
        self.recv_func = None

    async def recv_files(self):
        self.state = TransferState.CONNECTING
        self.connection = await self.get_connection()
        _logger.debug(f"{self._log_prefix} changing state to RECEIVING")
        self.state = TransferState.RECEIVING

        while True:
            if not await self._should_proceed():
                break

            self.current_file = await self._recv_file_item()
            self.file_items.append(self.current_file)
            async with aclosing(self._receive_single_file()) as file_receiver:
                try:
                    async for received_size in file_receiver:
                        if self.should_yield():
                            yield self.current_file, received_size
                except TransferIncomplete:
                    _logger.error(f"{self._log_prefix} paused receiving, changing state to PAUSED", exc_info=True)
                    self.state = TransferState.PAUSED
                    raise

        self.state = TransferState.COMPLETED
        _logger.info(f'completed receiving: {self.file_items}')

    async def _should_proceed(self):

        if self.to_stop:
            return False

        what = await self.recv_func(1)

        # check again, what if context switch happened
        if self.to_stop:
            _logger.debug(f"{self._log_prefix} found self.to_stop true, finalizing file recv loop")
            _logger.debug(f"{self._log_prefix} changing state to ABORTING")
            self.state = TransferState.ABORTING
            return False

        if not what:
            return False

        if not struct.unpack('?', what)[0]:
            _logger.info(
                f"{self._log_prefix} received end of transfer signal, finalizing file recv loop, changing state to COMPLETED")
            self.state = TransferState.COMPLETED
            return False

        return True

    async def _recv_file_item(self):
        raw_int = await self.recv_func(4)
        file_item_size = struct.unpack('!I', raw_int)[0]
        raw_file_item = await self.recv_func(file_item_size)
        file_item = FileItem.load_from(raw_file_item, self.download_path)
        return file_item

    async def _receive_single_file(self):
        validatename(file_item=self.current_file, root_path=self.download_path)
        receiver = recv_file_contents(self.recv_func, self.current_file, )
        initial = 0
        self.status_setup(f"[FILE] {self.current_file}", self.current_file.seeked, self.current_file.size)
        async with aclosing(receiver) as file_receiver:
            async for received_len in file_receiver:
                self.update_status(received_len - initial)
                initial = received_len
                if self.to_stop:
                    break
                yield

    async def get_connection(self):
        r = await self.connection_wait
        self.recv_func = connect.Receiver(r)
        self.send_func = connect.Sender(r)
        _logger.debug(f"got connection {self._file_id=}")
        return r

    async def continue_file_transfer(self):
        _logger.debug(f'FILE[{self._file_id}] changing state to receiving')
        self.state = TransferState.RECEIVING
        self.connection = await self.get_connection()
        seeked_ptr = struct.pack('!Q', self.current_file.seeked)

        # synchronizing last received file seek
        self.send_func(seeked_ptr)

        # receiving broken transfer file contents
        async with aclosing(self._receive_single_file()) as fr:
            async for received_len in fr:
                yield self.current_file, received_len

        # getting remaining files
        async with aclosing(self.recv_files()) as file_receiver:
            async for items in file_receiver:
                yield items

    def connection_arrived(self, connection):
        self.connection_wait.set_result(connection)

    @property
    def id(self):
        return self._file_id
