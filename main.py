import string
import random

import asyncio
import redis


SSLRequestCode = b'\x04\xd2\x16\x2f' # == hex(80877103)
StartupMessageCode = b'\x00\x03\x00\x00' # == hex(196608)

NoSSL = b'\x4E' # == 'N'

AuthenticationOk = b'\x52\x00\x00\x00\x08\x00\x00\x00\x00'
AuthenticationCleartextPassword = b'\x52\x00\x00\x00\x08\x00\x00\x00\x03'

ReadyForQuery = b'\x5A\x00\x00\x00\x05\x49' # == Z0005I , the last I stand for Idle 

Query = ord('Q')

def random_stream(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def CommandComplete(tag):
    lenghtTag = len(tag)
    lengthMessage = lenghtTag + 1 + 4 # one for the \00 and 4 for the Int32
    # assuming that tag is smaller than 256 bytes
    bytesLenght = b'\x00\x00\x00' + bytes([lengthMessage])
    bytesBody = bytes(tag, "utf-8") + b'\x00'
    return b'\x43' + bytesLenght + bytesBody

def RowDescription(rows):
    body = bytes(0)
    for rowType, rowName in rows:
        fieldName = bytes(rowName, "utf-8") + b'\x00'
        tableId = bytes(4)
        columnId = bytes(2)
        dataTypeId = bytes(4)
        if rowType == "int":
            dataSize = bytes([0, 8])
        if rowType == "string":
            dataSize = bytes([255, 255])
        typeModifier = bytes(4)
        formatCode = (0).to_bytes(2, byteorder="big")
        body += fieldName + tableId + columnId + dataTypeId + dataSize + typeModifier + formatCode
    totalLen = len(body) + 4 + 2
    totalLenBytes = totalLen.to_bytes(4, byteorder="big")
    totalFieldsBytes = len(rows).to_bytes(2, byteorder="big")
    return bytes([ord('T')]) + totalLenBytes + totalFieldsBytes + body

def DataRow(row):
    body = bytes(0)
    for fieldType, fieldValue in row.items():
        typeField, nameField = fieldType.decode("utf-8").split(":")
        if typeField == "int":
            value = fieldValue.decode()
            lenght = (len(value) + 1).to_bytes(4, byteorder="big")
            #valueBytes = value.to_bytes(8, byteorder="big")
            valueBytes = bytes(value, "utf-8") + b'\x00'
            body += lenght + valueBytes
    totalLen = len(body) + 4 + 2
    totalLenBytes = totalLen.to_bytes(4, byteorder="big")
    totalFieldsBytes = len(row).to_bytes(2, byteorder="big")
    return bytes([ord('D')]) + totalLenBytes + totalFieldsBytes + body

class PostgresProtocol(asyncio.Protocol):
    def __init__(self):
        self.redis = redis.Redis()
        self.state = "initial"
        self.db = "DB"

    def _execute_query(self, query):
        firstToken = query.split(' ')[0]
        if firstToken.upper() == "INSERT":
            result = self.redis.execute_command("REDISQL.EXEC", self.db , query)
            numberInserted = result[1]
            self.transport.write(CommandComplete("INSERT 0 " + str(numberInserted)))
        elif firstToken.upper() == "SELECT":
            stream = random_stream()
            result = self.redis.execute_command("REDISQL.QUERY.INTO", stream, self.db, query)
            streamResult = self.redis.execute_command("XREAD",  "COUNT", "1", "STREAMS", stream, "0")
            firstRow = streamResult[0][1][0][1]
            rows = []
            for key, _ in firstRow.items():
                rowType, rowName = key.decode("utf-8").split(':')
                rowType, rowName = rowType.strip(), rowName.strip()
                rows.append((rowType, rowName,))
            self.transport.write(RowDescription(rows))
            returnedRows = self.redis.xread({stream: "0"})[0][1]
            for _, row in returnedRows:
                self.transport.write(DataRow(row))
            self.transport.write(CommandComplete("SELECT"))
        else:
            self.redis.execute_command("REDISQL.EXEC", self.db , query)
            self.transport.write(CommandComplete(firstToken))

        self.transport.write(ReadyForQuery)


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
