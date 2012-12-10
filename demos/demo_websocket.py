#!/usr/bin/env python

""" Exmaple of websocket message handler for brubeck
Demo implements echo server that works over websockets.

Experiment with 
http://isr.nu/ws/WebSocketTest.htm
or
http://www.websocket.org/echo.html
"""

import logging

from brubeck.request_handling import http_response, MessageHandler, Brubeck
from brubeck.connections import Mongrel2Connection

def websocket_response(data, opcode=1, rsvd=0):
    """Renders arguments into WebSocket response
    """
    header = ''
    header += chr(0x80|opcode|rsvd<<4)
    realLength = len(data)
    if realLength < 126:
        dummyLength = realLength
    elif realLength < 2**16:
        dummyLength = 126
    else:
        dummyLength = 127
    header += chr(dummyLength)
    if dummyLength == 127:
        header += chr(realLength >> 56 & 0xff)
        header += chr(realLength >> 48 & 0xff)
        header += chr(realLength >> 40 & 0xff)
        header += chr(realLength >> 32 & 0xff)
        header += chr(realLength >> 24 & 0xff)
        header += chr(realLength >> 16 & 0xff)
    if dummyLength == 126 or dummyLength == 127:
        header += chr(realLength >> 8 & 0xff)
        header += chr(realLength & 0xff)
    return header + data


class WSMessageHandler(MessageHandler):
    """ Class implements websocket with as little dependancies as possible 
    """
    OPCODE_CONTINUATION = 0x0
    OPCODE_TEXT = 0x1
    OPCODE_BINARY = 0x2
    OPCODE_CLOSE = 0x8
    OPCODE_PING = 0x9
    OPCODE_PONG = 0xa

    def initialize(self):
        """Just pack out some flags
        """ 
        ### WebSocket message parsing
        if self.message.method == 'WEBSOCKET':
            flags = int(self.message.headers.get('FLAGS'),16)
            self.ws_final = flags & 0x80 == 0x80
            self.ws_rsvd = flags & 0x70
            self.ws_opcode = flags & 0xf

    def websocket_handshake(self):
        """Does initial handshake. This is a good place to register the websocket 
           connection in some memory database
        """
        headers = {}
        headers['Content-Type'] = 'text/plain'
        headers['Upgrade'] = 'websocket'
        headers['Connection'] = 'Upgrade'
        headers['Sec-WebSocket-Accept'] = self.message.body

        logging.info('%s %s %s (%s)' % (
             '101', self.message.method, self.message.path, self.message.remote_addr))
        return http_response('', '101', 'Switching Protocols', headers)

    def render(self, msg = None, opcode = 1, rsvd = 0, **kwargs):
        """Renders payload and prepares the payload for a successful WS
        response.
        """
        logging.info('%s %s %s (%s)' % (
             opcode, self.message.method, self.message.path, self.message.remote_addr))
        return websocket_response(msg, opcode, rsvd)

class EchoHandler(WSMessageHandler):
    def websocket(self):
        if self.ws_opcode == self.OPCODE_CLOSE:
            return self.render('', self.OPCODE_CLOSE)

        elif self.ws_opcode == self.OPCODE_PING:
            return self.render(self.message.body, self.OPCODE_PONG)

        elif self.ws_opcode == self.OPCODE_TEXT:
            msg = self.message.body.decode('utf-8')
            return self.render(msg.encode('utf-8'), self.OPCODE_TEXT)
        else:
            raise Exception('Unhandled opcode in WebsocketHandler')

urls = [(r'^/echo', EchoHandler)]

config = {
    'msg_conn': Mongrel2Connection('tcp://127.0.0.1:9999', 'tcp://127.0.0.1:9998'),
    'handler_tuples': urls,
}

app = Brubeck(**config)

app.run()
