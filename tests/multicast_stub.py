import asyncio


class Multicast(asyncio.DatagramProtocol):
    def __init__(self):
        self.all_peers = set()

    def connection_made(self, transport):
        print("server up and running", transport.get_extra_info('socket'))
        self.transport = transport

    def datagram_received(self, data, addr):
        print('new message arrived', addr)
        print("forwarding to", end=' ')
        for peer in (self.all_peers - {addr}):
            print(peer, end=' ')
            self.transport.sendto(data, peer)
        print('')
        self.all_peers.add(addr)


async def main():
    # multicast_ip = '127.0.0.1'
    multicast_ip = '172.16.210.0'
    multicast_port = 4000

    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        Multicast,
        local_addr=(multicast_ip, multicast_port)
    )
    await asyncio.Event().wait()

    print("stopping")


if __name__ == "__main__":
    asyncio.run(main=main())
