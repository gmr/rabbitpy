"""
Core IO for rabbitpy

"""
import errno
import logging
try:
    import queue
except ImportError:
    import Queue as queue
import select
import socket
import ssl
import threading

LOGGER = logging.getLogger(__name__)

from pamqp import frame
from pamqp import exceptions as pamqp_exceptions
from pamqp import specification

from rabbitpy import DEBUG
from rabbitpy import base
from rabbitpy import events
from rabbitpy import exceptions

MAX_READ = 4096
MAX_WRITE = 32
POLL_TIMEOUT = 3600.0


class IOLoop(object):

    NONE = 0
    READ = 0x001
    WRITE = 0x004
    ERROR = 0x008 | 0x010

    def __init__(self, fd, error_callback, read_callback, write_queue,
                 event_obj, write_trigger):
        self._data = threading.local()
        self._data.fd = fd
        self._data.error_callback = error_callback
        self._data.read_callback = read_callback
        self._data.write_queue = write_queue
        self._data.running = False
        self._data.failed_write = None
        self._data.ssl = hasattr(fd, 'read')
        self._data.events = event_obj
        self._data.write_trigger = write_trigger

        # Materialized lists for select
        self._data.read_only = [[fd, write_trigger], [], [fd], POLL_TIMEOUT]
        self._data.read_write = [[fd, write_trigger], [fd], [fd], POLL_TIMEOUT]

    def run(self):
        self._data.running = True
        while self._data.running:
            try:
                self._poll()
            except (KeyboardInterrupt, SystemExit) as exception:
                LOGGER.warning('Exiting IOLoop.run due to %s', exception)
                break
            except EnvironmentError as exception:
                if (getattr(exception, 'errno', None) == errno.EINTR or
                    (isinstance(getattr(exception, 'args', None), tuple) and
                     len(exception.args) == 2 and
                     exception.args[0] == errno.EINTR)):
                    continue

            if self._data.events.is_set(events.SOCKET_CLOSE):
                if DEBUG:
                    LOGGER.debug('Exiting due to closed socket')
                break
        if DEBUG:
            LOGGER.debug('Exiting IOLoop.run')

    def stop(self):
        LOGGER.info('Stopping IOLoop')
        self._data.running = False
        try:
            self._data.write_trigger.close()
        except socket.error:
            pass

    def _poll(self):
        # Poll select with the materialized lists
        #if DEBUG:
        #    LOGGER.debug('Polling')
        if not self._data.running:
            LOGGER.info('Exiting poll')

        if self._data.write_queue.empty() and not self._data.failed_write:
            read, write, err = select.select(*self._data.read_only)
        else:
            read, write, err = select.select(*self._data.read_write)
        if err:
            self._data.running = False
            self._data.error_callback(None)
            return

        # Clear out the trigger socket
        if self._data.write_trigger in read:
            self._data.write_trigger.recv(1024)

        if self._data.fd in read:
            self._read()
        if write:
            self._write()
        #if DEBUG:
        #    LOGGER.debug('End of poll')

    def _read(self):
        if not self._data.running:
            if DEBUG:
                LOGGER.debug('Skipping read, not running')
            return
        try:
            if self._data.ssl:
                self._data.read_callback(self._data.fd.read(MAX_READ))
            else:
                self._data.read_callback(self._data.fd.recv(MAX_READ))
        except socket.timeout:
            pass
        except socket.error as exception:
            self._data.running = False
            self._data.error_callback(exception)

    def _write(self):
        # If there is data that still needs to be sent, use it instead
        if self._data.failed_write:
            data = self._data.failed_write
            self._data.failed_write = None
            return self._write_frame(data[0], data[1])

        frames = 0
        while frames < MAX_WRITE:
            try:
                data = self._data.write_queue.get(False)
            except queue.Empty:
                break
            self._write_frame(data[0], data[1])
            frames += 1
            if self._data.write_queue.empty():
                break

    def _write_frame(self, channel, value):
        if not self._data.running:
            if DEBUG:
                LOGGER.debug('Skipping write frame, not running')
            return
        frame_data = frame.marshal(value, channel)
        try:
            self._data.fd.sendall(frame_data)
        except socket.timeout:
            self._data.failed_write = channel, value
        except socket.error as exception:
            self._data.running = False
            self._data.error_callback(exception)


class IO(threading.Thread, base.StatefulObject):

    CONNECTION_TIMEOUT = 3
    CONTENT_METHODS = ['Basic.Deliver', 'Basic.GetOk', 'Basic.Return']
    READ_BUFFER_SIZE = specification.FRAME_MAX_SIZE

    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = dict()
        super(IO, self).__init__(group, target, name, args, kwargs)

        self._args = kwargs['connection_args']
        self._events = kwargs['events']
        self._exceptions = kwargs['exceptions']
        self._write_queue = kwargs['write_queue']

        # A socket to trigger write interrupts with
        self._write_listener, self._write_trigger = self._socketpair()

        self._buffer = bytes()
        self._channels = dict()
        self._remote_name = None
        self._socket = None
        self._state = None
        self._loop = None

    def add_channel(self, channel, write_queue):
        """Add a channel to the channel queue dict for dispatching frames
        to the channel.

        :param rabbitpy.channel.Channel: The channel to add
        :param Queue.Queue write_queue: Queue for sending frames to the channel

        """
        self._channels[int(channel)] = channel, write_queue

    def run(self):
        """

        """
        try:
            self._run()
        except socket.error as exception:
            LOGGER.error('Exiting due to socket.error: %s', exception)
        except Exception as exception:
            LOGGER.error('Unhandled exception: %s', exception, exc_info=True)
            self._exceptions.put(exception)
            if self._events.is_set(events.CHANNEL0_OPENED):
                self._events.set(events.CHANNEL0_CLOSE)
                self._events.wait(events.CHANNEL0_CLOSED)
            if self.open:
                self._close()
            self._events.set(events.EXCEPTION_RAISED)

        del self._loop
        del self._socket
        del self._channels
        del self._buffer
        LOGGER.info('Exiting IO.run')

    def on_error(self, exception):
        """Common functions when a socket error occurs. Make sure to set closed
        and add the exception, and note an exception event.

        :param socket.error exception: The socket error

        """
        LOGGER.error('Socket error: %s', exception, exc_info=True)
        args = [self._args['host'], self._args['port'], exception[1]]
        if self._channels[0][0].open:
            self._exceptions.put(exceptions.ConnectionResetException(*args))
        else:
            self._exceptions.put(exceptions.ConnectionException(*args))
        self._set_state(self.CLOSED)
        self._loop.stop()
        try:
            self._write_trigger.send('0')
        except socket.error:
            pass
        self._events.clear(events.SOCKET_OPENED)
        self._events.set(events.SOCKET_CLOSED)
        self._events.set(events.EXCEPTION_RAISED)
        if self.open:
            self._close()
        self._channels[0][0]._set_state(self._channels[0][0].CLOSED)

    def on_read(self, data):
        # Append the data to the buffer
        self._buffer += data

        while self._buffer:

            # Read and process data
            value = self._read_frame()

            # Break out if a frame could not be decoded
            if self._buffer and value[0] is None:
                break

            if DEBUG:
                LOGGER.debug('Received (%i) %r', value[0], value[1])

            # If it's channel 0, call the Channel0 directly
            if value[0] == 0:
                self._channels[0][0].on_frame(value[1])
                continue

            # Close the channel if it's a Channel.Close frame
            if isinstance(value[1], specification.Channel.Close):
                self._remote_close_channel(value[0], value[1])

            # Let the channel know a message was returned
            elif isinstance(value[1], specification.Basic.Return):
                self._notify_of_basic_return(value[0], value[1])

            # Add the frame to the channel queue
            else:
                self._add_frame_to_queue(value[0], value[1])

    def stop(self):
        """Stop the IO Layer due to exception or other problem.

        """
        self._close()

    @property
    def write_trigger(self):
        return self._write_trigger

    def _add_frame_to_queue(self, channel_id, frame_value):
        """Add the frame to the stack by creating the key value used in
        expectations and then add it to the list.

        :param int channel_id: The channel id the frame was received on
        :param pamqp.specification.Frame frame_value: The frame to add

        """
        if DEBUG:
            LOGGER.debug('Adding %s to channel %s',
                         frame_value.name, channel_id)
        self._channels[channel_id][1].put(frame_value)

    def _close(self):
        self._set_state(self.CLOSING)
        if hasattr(self, '_socket') and self._socket:
            self._socket.close()
        self._events.clear(events.SOCKET_OPENED)
        self._events.set(events.SOCKET_CLOSED)
        self._set_state(self.CLOSED)

    def _connect_socket(self, sock, address):
        """Connect the socket to the specified host and port."""
        if DEBUG:
            LOGGER.debug('Connecting to %r', address)
        sock.settimeout(self.CONNECTION_TIMEOUT)
        sock.connect(address)

    def _connect(self):
        """Connect to the RabbitMQ Server

        :raises: ConnectionException

        """
        self._set_state(self.OPENING)
        sock = None
        for (af, socktype, proto,
             canonname, sockaddr) in self._get_addr_info():
            try:
                sock = self._create_socket(af, socktype, proto)
                self._connect_socket(sock, sockaddr)
                break
            except socket.error as error:
                if DEBUG:
                    LOGGER.debug('Error connecting to %r: %s', sockaddr, error)
                sock = None
                continue

        if not sock:
            args = [self._args['host'], self._args['port'],
                    'Could not connect']
            self._exceptions.put(exceptions.ConnectionException(*args))
            self._events.set(events.EXCEPTION_RAISED)
            return

        self._socket = sock
        self._socket.settimeout(0)
        self._events.set(events.SOCKET_OPENED)
        self._set_state(self.OPEN)

    def _create_socket(self, af, socktype, proto):
        """Create the new socket, optionally with SSL support.

        :rtype: socket.socket or ssl.SSLSocket

        """
        sock = socket.socket(af, socktype, proto)
        if self._args['ssl']:
            if DEBUG:
                LOGGER.debug('Wrapping socket for SSL')
            return ssl.wrap_socket(sock,
                                   self._args['ssl_key'],
                                   self._args['ssl_cert'],
                                   False,
                                   self._args['ssl_validation'],
                                   self._args['ssl_version'],
                                   self._args['ssl_cacert'])
        return sock

    def _disconnect_socket(self):
        """Close the existing socket connection"""
        self._socket.close()

    def _get_addr_info(self):
        family = socket.AF_UNSPEC
        if not socket.has_ipv6:
            family = socket.AF_INET
        try:
            res = socket.getaddrinfo(self._args['host'],
                                     self._args['port'],
                                     family,
                                     socket.SOCK_STREAM,
                                     0)
        except socket.error as error:
            LOGGER.error('Could not resolve %s: %s',
                         self._args['host'], error, exc_info=True)
            return []
        return res

    @staticmethod
    def _get_frame_from_str(value):
        """Get the pamqp frame from the string value.

        :param str value: The value to parse for an pamqp frame
        :return (str, int, pamqp.specification.Frame): Remainder of value,
                                                       channel id and
                                                       frame value
        """
        if not value:
            return value, None, None
        try:
            byte_count, channel_id, frame_in = frame.unmarshal(value)
        except pamqp_exceptions.UnmarshalingException:
            return value, None, None
        except specification.AMQPFrameError as error:
            LOGGER.error('Failed to demarshal: %r', error, exc_info=True)
            if DEBUG:
                LOGGER.debug(value)
            return value, None, None
        return value[byte_count:], channel_id, frame_in

    def _notify_of_basic_return(self, channel_id, frame_value):
        """Invoke the on_basic_return code in the specified channel. This will
        block the IO loop unless the exception is caught.

        :param int channel_id: The channel for the basic return
        :param pamqp.specification.Frame frame_value: The Basic.Return frame

        """
        self._channels[channel_id][0].on_basic_return(frame_value)

    def _read_frame(self):
        """Read from the buffer and try and get the demarshaled frame.

        :rtype (int, pamqp.specification.Frame): The channel and frame

        """
        self._buffer, chan_id, value = self._get_frame_from_str(self._buffer)
        return chan_id, value

    def _remote_close_channel(self, channel_id, frame_value):
        """Invoke the on_channel_close code in the specified channel. This will
        block the IO loop unless the exception is caught.

        :param int channel_id: The channel to remote close
        :param pamqp.specification.Frame frame_value: The Channel.Close frame

        """
        self._channels[channel_id][0].on_remote_close(frame_value)

    def _run(self):
        """Start the thread, which will connect to the socket and run the
        event loop.

        """
        self._connect()
        if DEBUG:
            LOGGER.debug('Socket connected')

        # Create the remote name
        address, port = self._socket.getsockname()
        peer_address, peer_port = self._socket.getpeername()
        self._remote_name = '%s:%s -> %s:%s' % (address, port,
                                                peer_address, peer_port)
        self._loop = IOLoop(self._socket, self.on_error, self.on_read,
                            self._write_queue,
                            self._events,
                            self._write_listener)
        self._loop.run()

    def _socketpair(self):
        """Return a socket pair regardless of platform.

        :rtype: (socket, socket)

        """
        try:
            server, client = socket.socketpair()
        except AttributeError:
            # Connect in Windows
            LOGGER.debug('Falling back to emulated socketpair behavior')

            # Create the listening server socket & bind it to a random port
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('127.0.0.1', 0))

            # Get the port for the notifying socket to connect to
            port = s.getsockname()[1]

            # Create the notifying client socket and connect using a timer
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            def connect():
                client.connect(('127.0.0.1', port))

            t = threading.Timer(0.01, connect)
            t.start()

            # Have the listening server socket listen and accept the connect
            s.listen(0)
            server, _unused = s.accept()

        # Don't block on either socket
        server.setblocking(0)
        client.setblocking(0)
        return server, client
