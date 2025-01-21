"""

## How do we perform

1. A distributed search
2. Gather list of peers to display

in a p2p network, working with kademila's routing protocol?

### 1.1 If user can enter the peer id,
    then we can go through the network in O(log n),
    and get that peer details
    cons - But it's the worst UX

### 1.2 User enters a search string related to what ever he knows about other peer
    - we use that string to relate to peer's info and get that (from where?)

### 1.3 refer `2.3`

### 1.4 perform a gossip-based search
    - send a search request packet to the network
    - maintain a state for that request in the memory
    - if a peer relates to that string, it replies to that search request by sending a datagram containing it's peer object
    - gather all the replies and cache them in `Storage`

    pros :
    - fast and efficient
    - can we scaled and pretty decentralized
    - cool

    cons :
    - no control over the search query (like cancelling that query) once it is passed into network
    - whole network will eventually respond to that search query
    - we have to ignore those packets


### 2.1 We need to display list of peers who are active in the network
    *.1 Need to iterate over the network over kademlia's routing protocol,
        for that we have to get the first id node in the network,
        append all the nodes found to a priority queue marking them visited if already queried,
        (sounds dfs or bfs), cache all that locally for a while

        cons - we have so much redundant peer objects passing here and there

    *.2 Full brute force
        we have some details of our logically nearest peers,
        so we brute force that list and get whatever they have
        again query the list of peers sent by them in loop

        cons - we have so much redundant peer objects passing here and there

### 2.2 Preferred solution
    - we selected 20 buckets spaced evenly in the { 0 - 2 ^ 160 } node id space
    - give the bucket authority to nearest peer (closer to that bucket id)
    - each peer's adds themselves to that bucket when they join the network
        - peer's even ping the bucket and re-enter themselves within a time window

    - if a peer owns the bucket, another peer joins the network with more closest id to the bucket
      then a redistribution of bucket to that nearest peer will happen and all the authority over that bucket is
      transferred
    - problem, what if someone is querying that bucket in the meanwhile?
    (we can say that this is consistent hashing)

    - we can now show list of peer's available in the network by,
    step 1 : iterating over the `list of bucket id's`,
    step 2 : communicating  to the peer that is responsible for that bucket
    step 3 : perform a get list of peers RPC
    step 4 : show the list of peers to user

    - now we have peer gathering feature with paging

### 2.3

referring 1.3 :
    we can iterate over `list of bucket id's`,
    communicate to the peers that own bucket,
    ask them to search for relevant peers that match the given search string,
    return the list of peer's matched

    never dos:
        - cache the owner peer for a respective bucket as they can change pretty fast,
          always use kademlia's search protocol to get latest peer data
        - permanently cache peer's data received

"""
import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Optional, override

from kademlia import crawling, storage

from src.avails import DataWeaver, GossipMessage, RemotePeer, WireData, connect, const, use
from src.avails.remotepeer import convert_peer_id_to_byte_id
from src.avails.useables import get_unique_id
from src.core import DISPATCHS, Dock, get_gossip, get_this_remote_peer
from src.transfers import GOSSIP, HEADERS
from src.webpage_handlers import pagehandle
from src.webpage_handlers.headers import HANDLE

_logger = logging.getLogger(__name__)

# this list contains 20 evenly spread numbers in [0, 2**160]
node_list_ids = [
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
    b'\x0c\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc',
    b'\x19\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x98',
    b'&ffffffffffffffffffd',
    b'33333333333333333330',
    b'?\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfc',
    b'L\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xc8',
    b'Y\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x94',
    b'fffffffffffffffffff`',
    b's333333333333333333,',
    b'\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf8',
    b'\x8c\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xc4',
    b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x90',
    b'\xa6ffffffffffffffffff\\',
    b'\xb3333333333333333333(',
    b'\xbf\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf4',
    b'\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xc0',
    b'\xd9\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x99\x8c',
    b'\xe6ffffffffffffffffffX',
    b'\xf3333333333333333333$',
]


class Storage(storage.ForgetfulStorage):
    # :todo: introduce diff based reads
    node_lists_ids = set(node_list_ids)
    peer_data_storage = defaultdict(set)

    def get_list_of_peers(self, list_key):
        if list_key in self.peer_data_storage:
            return list(self.peer_data_storage.get(list_key))

    def all_peers_in_lists(self):
        return self.peer_data_storage.items()

    def store_peers_in_list(self, list_key, list_of_peers):
        if list_key not in self.node_lists_ids:
            return False

        # temporary fix
        filtered_peers = set()
        for peer in list_of_peers:
            if isinstance(peer, list):
                filtered_peers.add(peer[0])
            else:
                filtered_peers.add(peer)
        self.peer_data_storage[list_key] |= filtered_peers

        return True


class SearchCrawler:

    @classmethod
    async def get_relevant_peers_for_list_id(cls, kad_server, list_id):
        peer = RemotePeer(list_id)
        nearest = kad_server.protocol.router.find_neighbors(peer)
        crawler = crawling.NodeSpiderCrawl(
            kad_server.protocol,
            peer,
            nearest,
            kad_server.ksize,
            kad_server.alpha
        )
        responsible_nodes = await crawler.find()
        return responsible_nodes

    @classmethod
    async def search_for_nodes(cls, node_server, search_string):
        for peer in use.search_relevant_peers(Dock.peer_list, search_string):
            yield peer

        for list_id in node_list_ids:
            peers = await cls.get_relevant_peers_for_list_id(node_server, list_id)
            for peer in peers:
                _peers = await node_server.protocol.call_search_peers(peer, search_string)
                yield _peers


class PeerListGetter(crawling.ValueSpiderCrawl):
    peers_cache = {}
    previously_fetched_index = 0

    async def find(self):
        return await self._find(self.protocol.call_find_peer_list)

    @override
    async def _handle_found_values(self, values):
        peer = self.nearest_without_value.popleft()
        if peer:
            _logger.debug(f"found values {values}")
            await self.protocol.call_store_peers_in_list(peer, self.node.id, values)
        return values

    @classmethod
    async def get_more_peers(cls, peer_server) -> list[RemotePeer]:
        _logger.debug(f"previous index {cls.previously_fetched_index}")
        if cls.previously_fetched_index >= len(node_list_ids) - 1:
            cls.previously_fetched_index = 0

        find_list_id = node_list_ids[cls.previously_fetched_index]
        _logger.debug(f"looking into {find_list_id}")
        list_of_peers = await peer_server.get_list_of_nodes(find_list_id)
        cls.previously_fetched_index += 1

        if list_of_peers:
            cls.peers_cache.update({x.peer_id: x for x in list_of_peers})

            return list(set(list_of_peers))

        return []


class GossipSearch:
    class search_iterator(AsyncIterator):
        timeout = 3

        def __init__(self, message_id):
            self.message_id = message_id
            self.reply_queue: asyncio.Queue[RemotePeer] = asyncio.Queue()

        def add_peer(self, p):
            self.reply_queue.put_nowait(p)

        def __aiter__(self):
            self._start_time = asyncio.get_event_loop().time()
            return self

        async def __anext__(self):
            current_time = asyncio.get_event_loop().time()
            if current_time - self._start_time > self.timeout:
                raise StopAsyncIteration

            try:
                search_response = await asyncio.wait_for(self.reply_queue.get(),
                                                         timeout=self.timeout - (current_time - self._start_time))
                return search_response
            except asyncio.TimeoutError:
                raise StopAsyncIteration

    _message_state_dict: dict[str, search_iterator] = {}
    _search_cache = {}

    @classmethod
    def search_for(cls, find_str, gossip_handler):
        _logger.info(f"[GOSSIP][SEARCH] new search for: {find_str}")
        m = cls._prepare_search_message(find_str)
        gossip_handler.gossip_message(m)
        cls._message_state_dict[m.id] = f = cls.search_iterator(m.id)
        return f

    @classmethod
    def request_arrived(cls, req_data: GossipMessage, addr):
        search_string = req_data.message
        me = get_this_remote_peer()
        if me.is_relevant(search_string):
            return cls._prepare_reply(req_data.id)

    @staticmethod
    def _prepare_reply(reply_id):
        gm = GossipMessage()
        gm.header = GOSSIP.SEARCH_REPLY
        gm.id = reply_id
        gm.message = get_this_remote_peer().serialized
        gm.created = time.time()
        return bytes(gm)

    @staticmethod
    def _prepare_search_message(find_str):
        gm = GossipMessage()
        gm.header = GOSSIP.SEARCH_REQ
        gm.message = find_str
        gm.id = get_unique_id()
        gm.created = time.time()
        return gm

    @classmethod
    def reply_arrived(cls, reply_data: GossipMessage, addr):
        Dock.global_gossip.message_arrived(reply_data, addr)
        try:
            result_iter = cls._message_state_dict[reply_data.id]
            if m := reply_data.message:
                m = RemotePeer.load_from(m)
            result_iter.add_peer(m)
        except KeyError as ke:
            _logger.debug("[GOSSIP][SEARCH] invalid gossip search response id", exc_info=ke)


def get_search_handler():
    return GossipSearch


def get_more_peers():
    peer_server = Dock.kademlia_network_server
    _logger.debug("getting more peers")
    return PeerListGetter.get_more_peers(peer_server)


async def gossip_search(search_string) -> AsyncIterator[RemotePeer]:
    searcher = get_search_handler()
    async for peer in searcher.search_for(search_string, get_gossip()):
        yield peer


def search_for_nodes_with_name(search_string):
    """
    searches for nodes relevant to given ``:param search_string:``

    Returns:
         a generator of peers that matches with the search_string
    """
    peer_server = Dock.kademlia_network_server
    return SearchCrawler.search_for_nodes(peer_server, search_string)


async def get_remote_peer(peer_id):
    """Gets the ``RemotePeer`` object corresponding to ``:func RemotePeer.peer_id:`` from the network

    Just a wrapper around ``:method kademlia_network_server.get_remote_peer:``
    with conversions related to ids

    This call is expensive as it performs a distributed search across the network
    try using ``Dock.peer_list`` instead if possible

    Args:
        peer_id(str): id to search for

    Returns:
        RemotePeer | None
    """
    byte_id = convert_peer_id_to_byte_id(peer_id)
    return await Dock.kademlia_network_server.get_remote_peer(byte_id)


async def get_remote_peer_at_every_cost(peer_id) -> Optional[RemotePeer]:
    """
    Just a helper, tries to check for peer_id in cached Dock.peer_list
    if there is a chance that cached remote peer object is expired then use :func: `peers.get_remote_peer`
    if not found the performs a distributed search in the network
    """
    try:
        peer_obj = Dock.peer_list.get_peer(peer_id)
    except KeyError:
        peer_obj = await get_remote_peer(peer_id)

    return peer_obj


# Callbacks called by kademila's routing mechanisms

def new_peer(peer):
    Dock.peer_list.add_peer(peer)
    data = DataWeaver(
        header=HANDLE.NEW_PEER,
        content={
            "name": peer.username,
            "ip": peer.ip,
        },
        peer_id=peer.peer_id,
    )
    pagehandle.dispatch_data(data)


async def check_and_remove(peer: RemotePeer):
    req_dispatcher = Dock.dispatchers[DISPATCHS.REQUESTS]
    ping_data = WireData(
        header=HEADERS.REMOVAL_PING,
        msg_id=use.get_unique_id(str)
    )
    fut = req_dispatcher.register_reply(ping_data.id)  # noqa
    _logger.debug(f"connectivity check initiating for {peer}")
    try:
        await asyncio.wait_for(fut, const.PING_TIMEOUT)
    except TimeoutError:
        # try a tcp connection if network is terrible with UDP
        try:
            with await connect.connect_to_peer(peer):
                pass
        except (OSError, TimeoutError):
            # okay this one is cooked
            Dock.peer_list.remove_peer(peer.peer_id)
            data = DataWeaver(
                header=HANDLE.REMOVE_PEER,
                peer_id=peer.peer_id,
            )
            pagehandle.dispatch_data(data)
            _logger.info(f"connectivity check failed, removing peer {peer} from cache")


def remove_peer(peer):
    _logger.warning(f"a request for removal of {peer}")
    asyncio.create_task(check_and_remove(peer))
