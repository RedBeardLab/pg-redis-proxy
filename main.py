import asyncio

SSLRequestCode = b'\x04\xd2\x16\x2f' # == hex(80877103)
StartupMessageCode = b'\x00\x03\x00\x00' # == hex(196608)

NoSSL = b'\x4E' # == 'N'

AuthenticationOk = b'\x52\x00\x00\x00\x08\x00\x00\x00\x00'
AuthenticationCleartextPassword = b'\x52\x00\x00\x00\x08\x00\x00\x00\x03'

ReadyForQuery = b'\x5A\x00\x00\x00\x05\x49' # == Z0005I , the last I stand for Idle 

class PostgresProtocol(asyncio.Protocol):
    def __init__(self):
        self.state = "initial"

    def _reply(self, data):
        if self.state == "initial" and data[4:8] == SSLRequestCode:
            self.transport.write(NoSSL)
        elif self.state == "initial" and data[4:8] == StartupMessageCode:
            # we don't require a password
            self.transport.write(AuthenticationOk)
            # good to go for the first query!
            self.transport.write(ReadyForQuery)
        return

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        print('Data received: {!r}'.format(bytearray(data)))
        self._reply(data)

    def connection_lost(self, exc):
        print('The server closed the connection')
        print('Stop the event loop')

loop = asyncio.get_event_loop()
coro = loop.create_server(PostgresProtocol, '127.0.0.1', 8888)
server = loop.run_until_complete(coro)
loop.run_forever()
