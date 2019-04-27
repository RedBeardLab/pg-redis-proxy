import asyncio
import redis

SSLRequestCode = b'\x04\xd2\x16\x2f' # == hex(80877103)
StartupMessageCode = b'\x00\x03\x00\x00' # == hex(196608)

NoSSL = b'\x4E' # == 'N'

AuthenticationOk = b'\x52\x00\x00\x00\x08\x00\x00\x00\x00'
AuthenticationCleartextPassword = b'\x52\x00\x00\x00\x08\x00\x00\x00\x03'

ReadyForQuery = b'\x5A\x00\x00\x00\x05\x49' # == Z0005I , the last I stand for Idle 

Query = ord('Q')

def CommandComplete(tag):
    lenghtTag = len(tag)
    lengthMessage = lenghtTag + 1 + 4 # one for the \00 and 4 for the Int32
    # assuming that tag is smaller than 256 bytes
    bytesLenght = b'\x00\x00\x00' + bytes([lengthMessage])
    bytesBody = bytes(tag, "utf-8") + b'\x00'
    return b'\x43' + bytesLenght + bytesBody

def ExecuteQuery(query):
    firstToken = query.split(' ')[0]
    return firstToken

class PostgresProtocol(asyncio.Protocol):
    def __init__(self):
        self.redis = redis.Redis()
        self.state = "initial"

    def _execute_query(self, query):
        result = self.redis.execute_command("REDISQL.EXEC", "DB", query)
        firstToken = query.split(' ')[0]
        if firstToken.upper() == "INSERT":
            numberInserted = result[1]
            return "INSERT 0 " + str(numberInserted)
        return firstToken

    def _reply(self, data):
        if self.state == "initial" and data[4:8] == SSLRequestCode:
            self.transport.write(NoSSL)
        elif self.state == "initial" and data[4:8] == StartupMessageCode:
            # we don't require a password
            self.transport.write(AuthenticationOk)
            # good to go for the first query!
            self.transport.write(ReadyForQuery)
            self.state = "readyForQuery"
        if self.state == "readyForQuery" and data[0] == Query:
            lenght = int.from_bytes(data[1:5], "big")
            strLenght = lenght - 4
            query = data[5:-1].decode("utf-8")
            result = self._execute_query(query)
            self.transport.write(CommandComplete(result))
            self.transport.write(ReadyForQuery)
        return

    def connection_made(self, transport):
        print('New Connection Made')
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
