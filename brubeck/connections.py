import ujson as json
from uuid import uuid4
import cgi
import logging

from request import to_bytes, Request
from request_handling import coro_spawn


###
### Connection Classes
###

class Connection(object):
    """This class is an abstraction for how Brubeck sends and receives
    messages. The idea is that Brubeck waits to receive messages for some work
    and then it responds. Therefore each connection should essentially be a
    mechanism for reading a message and a mechanism for responding, if a
    response is necessary.
    """

    def process_message(self, application, message):
        """This coroutine looks at the message, determines which handler will
        be used to process it, and then begins processing.

        The application is responsible for handling misconfigured routes.
        """
        pass

    def _recv_forever_ever(self, fun_forever):
        """Calls a handler function that runs forever. The handler can be
        interrupted with a ctrl-c, though.
        """
        try:
            fun_forever()
        except KeyboardInterrupt, ki:
            # Put a newline after ^C
            print '\nBrubeck going down...'


###
### ZeroMQ
###

def load_zmq():
    """This function exists to determine where zmq should come from and then
    cache that decision at the module level.
    """
    if not hasattr(load_zmq, '_zmq'):
        from request_handling import CORO_LIBRARY
        if CORO_LIBRARY == 'gevent':
            from gevent_zeromq import zmq
        elif CORO_LIBRARY == 'eventlet':
            from eventlet.green import zmq
        load_zmq._zmq = zmq

    return load_zmq._zmq


def load_zmq_ctx():
    """This function exists to contain the namespace requirements of generating
    a zeromq context, while keeping the context at the module level. If other
    parts of the system need zeromq, they should use this function for access
    to the existing context.
    """
    if not hasattr(load_zmq_ctx, '_zmq_ctx'):
        zmq = load_zmq()
        zmq_ctx = zmq.Context()
        load_zmq_ctx._zmq_ctx = zmq_ctx

    return load_zmq_ctx._zmq_ctx


###
### Mongrel2
###

class Mongrel2Connection(Connection):
    """This class is an abstraction for how Brubeck sends and receives
    messages. This abstraction makes it possible for something other than
    Mongrel2 to be used easily.
    """
    MAX_IDENTS = 100

    def __init__(self, pull_addr, pub_addr):
        """sender_id = uuid.uuid4() or anything unique
        pull_addr = pull socket used for incoming messages
        pub_addr = publish socket used for outgoing messages

        The class encapsulates socket type by referring to it's pull socket
        as in_sock and it's publish socket as out_sock.
        """
        zmq = load_zmq()
        ctx = load_zmq_ctx()

        self.sender_id = uuid4().hex
        self.in_sock = ctx.socket(zmq.PULL)
        self.out_sock = ctx.socket(zmq.PUB)

        self.in_addr = pull_addr
        self.out_addr = pub_addr

        self.in_sock.connect(pull_addr)
        self.out_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        self.out_sock.connect(pub_addr)

    def process_message(self, application, message):
        """This coroutine looks at the message, determines which handler will
        be used to process it, and then begins processing.

        The application is responsible for handling misconfigured routes.
        """
        request = Request.parse_msg(message)
        if request.is_disconnect():
            return  # Ignore disconnect msgs. Dont have a reason to do otherwise
        handler = application.route_message(request)
        result = handler()
        # in case of long poll we do not want to send reply
        if result is not None:
            self.reply(request, result)

    def recv_forever_ever(self, application):
        """Defines a function that will run the primary connection Brubeck uses
        for incoming jobs. This function should then call super which runs the
        function in a try-except that can be ctrl-c'd.
        """
        def fun_forever():
            while True:
                request = self.in_sock.recv()
                coro_spawn(self.process_message, application, request)
        self._recv_forever_ever(fun_forever)

    def send(self, uuid, conn_id, msg):
        """Raw send to the given connection ID at the given uuid, mostly used
        internally.
        """
        header = "%s %d:%s," % (uuid, len(str(conn_id)), str(conn_id))
        self.out_sock.send(header + ' ' + to_bytes(msg))

    def reply(self, req, msg):
        """Does a reply based on the given Request object and message.
        """
        self.send(req.sender, req.conn_id, msg)

    def reply_bulk(self, uuid, idents, data):
        """This lets you send a single message to many currently
        connected clients.  There's a MAX_IDENTS that you should
        not exceed, so chunk your targets as needed.  Each target
        will receive the message once by Mongrel2, but you don't have
        to loop which cuts down on reply volume.
        """
        self.send(uuid, ' '.join(idents), data)

    def close(self):
        """Tells mongrel2 to explicitly close the HTTP connection.
        """
        self.reply(req.sender, "")

    def close_bulk(self, uuid, idents):
        """Same as close but does it to a whole bunch of idents at a time.
        """
        self.reply_bulk(uuid, idents, "")


###
### WSGI
###

class WSGIConnection(Connection):
    """
    """

    def __init__(self, port=6767):
        self.port = port

    def process_message(self, application, environ, callback):
        request = Request.parse_wsgi_request(environ)
        handler = application.route_message(request)
        result = handler()

        wsgi_status = ' '.join([str(result['status_code']), result['status_msg']])
        headers = [(k, v) for k,v in result['headers'].items()]
        callback(str(wsgi_status), headers)

        return [to_bytes(result['body'])]

    def recv_forever_ever(self, application):
        """Defines a function that will run the primary connection Brubeck uses
        for incoming jobs. This function should then call super which runs the
        function in a try-except that can be ctrl-c'd.
        """
        def fun_forever():
            from brubeck.request_handling import CORO_LIBRARY
            print "Serving on port %s..." % (self.port)

            def proc_msg(environ, callback):
                return self.process_message(application, environ, callback)

            if CORO_LIBRARY == 'gevent':
                from gevent import wsgi
                server = wsgi.WSGIServer(('', self.port), proc_msg)
                server.serve_forever()

            elif CORO_LIBRARY == 'eventlet':
                import eventlet.wsgi
                server = eventlet.wsgi.server(eventlet.listen(('', self.port)),
                                              proc_msg)

        self._recv_forever_ever(fun_forever)
