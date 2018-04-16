from socket import *
import sys
from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO
import httplib
import traceback
from threading import Thread
from threading import Lock
import Queue
import token_bucket
import argparse
import logging
from datetime import datetime, timedelta
import time
import errno
import select
from bidict import bidict
import os

###############################################################################
#                          CONSTANTS & GLOBALS                                #
###############################################################################

# max number of bytes that can be in an HTTP header
# NEEDSWORK: Can this cover requests with bodies? (eg POST)
MAX_HEADER_BYTES = 8000
# number of threads to handle requests
MAX_NUM_THREADS = 20

# list of HTTP requests to be handled
# items in the queue are tuples: (connection, client_address)
HTTP_requests = Queue.Queue()

# associate HTTPS client and server connections
HTTPS_server_by_client = bidict()  # key is HTTPS client connection

# determine if we should reuse server connections across different requests
REUSE_HTTP_CONNECTIONS = False
# list of open socket connections to reuse when connecting to the server
#   key: hostname
#   value: [(connection, TTL)]
# value cannot be an empty list - if the key exists then there must be at least
# one available connection
# TTL specifies when to close the connection
HTTP_servers = dict()
# ALWAYS use servers_lock when accessing the HTTP_servers dict
servers_lock = Lock()

# rate limiting mechanism (token bucket algo)
# rate: Number of tokens per second to add to the
#   bucket. Over time, the number of tokens that can be
#   consumed is limited by this rate. Each token represents
#   some percentage of a finite resource that may be
#   utilized by a consumer.
# capacity: Maximum number of tokens that the bucket
#   can hold. Once the bucket is full, additional tokens
#   are discarded.

# Rate and capacity units: number of requests (per client)
RATE = 100
CAPACITY = 500
storage = token_bucket.MemoryStorage()
limiter = token_bucket.Limiter(RATE, CAPACITY, storage)

# Choosing the verbose option prints every debug statement. Otherwise,
# only major ones are printed
VERBOSE = False

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] (%(threadName)-9s) %(message)s', datefmt='%m-%d %H:%M:%S')

class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = StringIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()
        self.body = None
        # TODO Find a better way to tell if there is a body to parse. So far
        # I only see POST and PUT as having bodies. Are these definitely the
        # only two?
        if self.command == 'POST' or self.command == 'PUT':
            content_len = int(self.headers.getheader('content-length', 0))
            self.body = self.rfile.read(content_len)

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message


###############################################################################
#                                   MAIN                                      #
###############################################################################

def main():
    args = handle_command_line_args()
    msock = create_master_socket(args)
    spawn_helper_threads()

    inputs = [msock]

    while inputs:
        readable, _, _ = select.select(inputs, [], [])

        closed_sockets = []

        for s in readable:
            if s is msock:
                new_connections = accept_new_connection(s)
                if new_connections:
                    inputs = inputs + new_connections
            elif s not in closed_sockets:
                try:
                    data = s.recv(1024)
                except error, v:
                    data = None
                    logging.debug( "********** Caught exception %s in main" % (str(sys.exc_info())))
                    if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))

                if data:
                    forward_bytes(s, data)
                else:
                    logging.debug("closing HTTPS connections")
                    print_HTTPS_server_by_client_bidict()
                    # remove client-server connection pair from HTTPS_server_by_client
                    s_prime = HTTPS_server_by_client.get(s)
                    if s_prime:
                        # s is a client connection, s_prime is a server connection
                        HTTPS_server_by_client.pop(s)
                    else:
                        # s in a server connection, s_prime is a client connection
                        s_prime = HTTPS_server_by_client.inv[s]
                        HTTPS_server_by_client.pop(s_prime)

                    # Stop listening for input on the connections
                    inputs.remove(s)
                    inputs.remove(s_prime)
                    # Close the connections
                    s.close()
                    s_prime.close()
                    # Don't try to read from the connection that we just closed
                    # This fixes the bad file descriptor errors & cascading issues with attempting to remove
                    # non-existent items from HTTPS_server_by_client
                    closed_sockets.append(s_prime)


def handle_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="print all debug messages", action="store_true")
    parser.add_argument("-r", "--reuse_connections", help="reuse connections where possible", action="store_true")
    parser.add_argument("-a", "--server_address", help="server's address (eg: localhost)", type=str, default="localhost")
    parser.add_argument("port", help="port to run on", type=int)
    args = parser.parse_args()

    if args.verbose:
        VERBOSE = True
    if args.reuse_connections:
        REUSE_HTTP_CONNECTIONS = True
    return args


def create_master_socket(args):
    msock = socket(AF_INET, SOCK_STREAM)
    # TODO: Test server_address arg with non-localhost option
    server_address = (args.server_address, args.port)

    logging.debug('starting up on %s port %s (%s, %s)' % 
        (server_address[0], server_address[1],
        "verbose logging" if VERBOSE else "regular (nonverbose) logging",
        "reusing connections" if REUSE_HTTP_CONNECTIONS else
        "using new connections each request"))

    msock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    msock.bind(server_address)
    msock.listen(1)

    return msock


def spawn_helper_threads():
    # create thread pool to manage HTTP requests
    threads = []
    for i in xrange(MAX_NUM_THREADS):
        t = Thread(target = consumer_thread, args = [i])
        t.setDaemon(True)
        threads.append(t)
        t.start()

    # spawn thread that periodically clears out the HTTP_servers dict
    # which allows us to reuse HTTP connections
    if REUSE_HTTP_CONNECTIONS:
        t = Thread(target = prune_HTTP_servers_dict, args = [])
        t.setDaemon(True)
        t.start()


# accept on the master socket
# returns a list of socket connections that need to be used
# as inputs to select()
def accept_new_connection(msock):

    # accept the new connection
    connection, client_address = msock.accept()
    logging.debug('=================== new connection from %s ===================' % str(client_address))
    connection.settimeout(0.5) # TODO should this be less?

    try:
        # read the first request and determine the protocol (HTTP or HTTPS)
        logging.debug('=============== on main thread, reading from %s ===============' % str(client_address))
        data = connection.recv(MAX_HEADER_BYTES)
        request = HTTPRequest(data)
        if VERBOSE: logging.debug('received "%s"' % data)
        if data:
            if request.command == "CONNECT":
                # the new conncetion is an HTTPS CONNECT request
                server_conn = setup_HTTPS_connection(connection, request)
                return [connection, server_conn]
            else:
                # the connection is an HTTP request
                HTTP_requests.put((connection, client_address, request))
    except error, v:
        # don't print timed out exceptions since they are an unpreventable error
        if v[0] !=  "timed out":
            logging.debug( "********** Caught exception: %s" % (str(sys.exc_info())))
            if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))
        # if we can't read from the new socket or we can't write to the new socket
        # something is very wrong, close and return
        connection.close()
        return None

# handle HTTPS CONNECT request
# return an HTTPS server connection
def setup_HTTPS_connection(client_conn, request):
    # send response to client
    reply = "HTTP/1.1 200 Connection established\r\n"  # TODO probably shouldn't hardcode this
    reply += "Proxy-agent: Pyx\r\n"
    reply += "\r\n"
    client_conn.sendall(reply.encode())
    logging.debug("sending connection est. to client")

    # setup HTTPS connection to server for client
    server_conn = socket(AF_INET, SOCK_STREAM)
    try:
        hostname, port = (request.headers["host"]).split(":")  # this could be fragile
    except:
        hostname = request.headers["host"]
        port = 443
    server_conn.connect((hostname, int(port)))

    # add server connection to two way dict. of HTTPS server connections
    HTTPS_server_by_client.put(client_conn, server_conn)
    return server_conn


def forward_bytes(connection, data):
    try:
        server = HTTPS_server_by_client.get(connection)
        if server:
            # data is from client
            if VERBOSE: logging.debug("Writing %d bytes to the server with fd %d" %
                                      (len(data), server.fileno()))
            server.sendall(data)
        else:
            # data is from server
            client = HTTPS_server_by_client.inv[connection]
            if VERBOSE: logging.debug("Writing %d bytes to the client with fd %d" %
                                      (len(data), client.fileno()))
            client.sendall(data)
            return
    except error, v:
        logging.debug( "********** Caught exception: %s" % (str(sys.exc_info())))
        if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))


def print_HTTPS_server_by_client_bidict():
    socketfds = []
    for key in HTTPS_server_by_client.keys():
        socketfds.append((key.fileno(), HTTPS_server_by_client.get(key).fileno()))
    socketfds.sort(key=lambda tup: tup[1])
    logging.debug("printing the HTTPS_server_by_client object, which has %d items..." %(len(socketfds)))
    logging.debug(socketfds)


###############################################################################
#                                 THREADING                                   #
###############################################################################
def consumer_thread(id):
    while True:
        request = HTTP_requests.get()
        handle_HTTP_request(*request)

def prune_HTTP_servers_dict():
    while(True):
        logging.debug("Number of requests queued = %d" % (HTTP_requests.qsize()))
        with servers_lock:
            curr_time = datetime.now()
            for hostname, connections in HTTP_servers.items():
                for index, conn in enumerate(connections):
                    if conn[1] < curr_time:
                        del connections[index]
                if connections:
                    HTTP_servers[hostname] = connections
                else:
                    del HTTP_servers[hostname]
            logging.debug("printing the server connections object")
            logging.debug(HTTP_servers)
        time.sleep(4)


###############################################################################
#                          CLIENT INTERACTIONS                                #
###############################################################################
def send_rate_limiting_error(connection, client_address, request):
    # sends an error message indicating the client is being
    # rate limited

    html_body = "<html><head><title>Too Many Requests</title></head><body> \
    <h1>Too Many Requests</h1> \
    <p>We currently allow only {} requests per second per IP address. Try again soon.</p> \
    </body></html>".format(RATE)

    formatted_res = 'HTTP/{} {} {}\r\n{}\r\n\r\n{}\r\n'.format(
        '1.0', 429, "Too Many Requests",
        'Content-Type: text/html',
        html_body
    )
    if VERBOSE: logging.debug(formatted_res)
    connection.sendall(formatted_res)

def handle_HTTP_request(connection, client_address, request):

    # handle first request
    response = get_response_from_server(request)
    send_response_to_client(response, connection)

    # if client sends another request, then handle it
    while True:
        try:
            if VERBOSE: logging.debug('=================== reading from %s ===================' % str(client_address))
            else: logging.debug("Reading from %s" % client_address)

            data = connection.recv(MAX_HEADER_BYTES)
            request = HTTPRequest(data)
            if VERBOSE: logging.debug('received "%s"' % data)
            if data:
                # Key is client address
                success = limiter.consume(client_address)
                if VERBOSE: logging.debug("CONSUMED TOKEN" if success else "COULD NOT CONSUME TOKEN")
                if not success:
                    logging.debug("Client %s is being rate limited" % client_address)
                    send_rate_limiting_error(connection, client_address, request)
                    # TODO: close connection here
                    break
                response = get_response_from_server(request)
                send_response_to_client(response, connection)
            else:
                logging.debug('no more data from %s' % str(client_address))
                break
        except (KeyboardInterrupt, SystemExit):
            if VERBOSE: logging.debug("Received KeyboardInterrupt or SystemExit, exiting now")
            sys.exit()
        except:
            logging.debug( "********** Caught exception: %s for client %s" % (str(sys.exc_info()), str(client_address)))
            if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))
            if 'BadStatusLine' in str(sys.exc_info()[1]):
                logging.debug("Reraising bad status line...")
                raise
            logging.debug('=================== CLOSING SOCKET ==================')
            connection.close()
            break


def send_response_to_client(response_lines, connection):
    for line in response_lines:
        connection.sendall(line)


###############################################################################
#                          SERVER INTERACTIONS                                #
###############################################################################

def acquire_server_connection(hostname):
    if REUSE_HTTP_CONNECTIONS:
        with servers_lock:
            connections = HTTP_servers.pop(hostname, None)
            if connections is not None:
                conn = connections.pop()
                if connections:
                    HTTP_servers[hostname] = connections
                return conn

    return (httplib.HTTPConnection(hostname), datetime.now())

def release_server_connection(hostname, conn, TTL, connection_close=False):
    if REUSE_HTTP_CONNECTIONS and not connection_close:
        # increase time to keep the connection open
        TTL = TTL + timedelta(seconds=5)
        if VERBOSE: logging.debug("Extending TTL for connection %s (Host: %s) to %s" % (conn, hostname, TTL))
        with servers_lock:
            if hostname in HTTP_servers:
                if VERBOSE: logging.debug("There were already %d connections for this host" % (len(server_connections[hostname])))
                HTTP_servers[hostname].append((conn, TTL))
            else:
                if VERBOSE: logging.debug("This is the only connection right now for this host")
                HTTP_servers[hostname] = [(conn, TTL)]
    else:
        logging.debug("Closing connection %s (host: %s)" % (conn, hostname))
        conn.close()

def read_response_content(response):
    response_lines = []
    headers = response.getheaders()
    http_version = '1.0' if response.version is 10 else '1.1'

    formatted_header = 'HTTP/{} {} {}\r\n{}\r\n\r\n'.format(
        http_version, str(response.status), response.reason,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in headers)
    )
    if VERBOSE: logging.debug("Adding headers to response")
    response_lines.append(formatted_header)
    if VERBOSE: logging.debug(formatted_header)

    # Chunked responses are forwarded using the approach found here:
    # https://stackoverflow.com/questions/24500752/how-can-i-read-exactly-one-response-chunk-with-pythons-http-client
    if response.getheader('transfer-encoding', '').lower() == 'chunked':
        if VERBOSE: logging.debug("Adding chunked body")

        def send_chunk_size():
            # size_str will contain the size of the current chunk in hex
            size_str = response.read(2)
            while size_str[-2:] != b"\r\n":
                size_str += response.read(1)

            if VERBOSE: logging.debug("chunk size: %s (%d) " % (size_str[:-2], int(size_str[:-2], 16)))

            # adds hex string plus \r\n delimeter to body
            response_lines.append(size_str)
            return int(size_str[:-2], 16)

        def send_chunk_data(chunk_size):
            # data for chunk + \r\n at the end
            data = response.read(chunk_size + 2)

            # TODO: Error case if data[-2:] != b"\r\n". Maybe throw a custom exception
            # that the parent thread can catch? Or just silently give up and rely on
            # eventual retries? Or maybe just manually add the delimeter? Not sure what
            # the best approach is.
            if data[-2:] != b"\r\n":
               logging.debug("ERROR: Chunk did not end in newline-carriage return")

            if VERBOSE: logging.debug("Data (first 20 bytes): %s" % data[:20])
            response_lines.append(data)

        while True:
            chunk_size = send_chunk_size()

            if (chunk_size == 0):
                if VERBOSE: logging.debug("Adding terminating chunk")
                # Sends the terminating \r\n and completes read of response
                response_lines.append(response.read(2))

                # Closes response so that if this connection is reused, the response will
                # will be cleared out. Otherwise this raises ResponseNotReady sometimes
                response.close()

                break
            else:
                if VERBOSE: logging.debug("Adding chunk of size %d " % chunk_size)
                send_chunk_data(chunk_size)

    else:
        if VERBOSE: logging.debug("Reading response in one go (not chunked)")
        #connection.sendall(response.read())
        response_lines.append(response.read())

    return response_lines

def get_response_from_server(request):
    hostname = request.headers["host"]

    logging.debug("Connecting to server with hostname %s" % hostname)
    conn, TTL = acquire_server_connection(hostname)
    if VERBOSE: logging.debug("Using connection %s (TTL: %s)" % (conn, TTL))
    

    logging.debug("Request path: %s. Split: %s" % (request.path, request.path.split(hostname)))

    # This logic is here because I got ' gaierror(-5, 'No address associated with hostname') '
    # when trying to make a request with the request.path.split(hostname)[1] logic.
    # I have not been able to reproduce the error, but this is a method I found that does the
    # same thing. I wanted to see, if this happened again, whether my method gets the same
    # result, and what specifically it gets.
    logging.debug("Alternatively... here's the other method:")
    url = request.path
    http_pos = url.find('://')
    if http_pos == -1:
        temp = url
    else:
        temp = url[(http_pos + 3):]

    port_pos = temp.find(':')

    webserver_pos = temp.find('/')
    if webserver_pos == -1:
        webserver_pos = len(temp)
    webserver = ''
    port = -1

    if port_pos == -1 or webserver_pos < port_pos:
        port = 80
        webserver = temp[:webserver_pos]

    else:
        port = int((temp[(port_pos + 1):])[:webserver_pos - port_pos -1])
        webserver = temp[:port_pos]
    logging.debug(webserver)
    # TODO: Chrome does not allow cookies to be set by localhost. Logins will not work with chrome.
    # TODO: Cookies seem to be acting up in Firefox too. I get logged out after a couple of page
    # changes
    if request.body:
        conn.request(request.command, request.path.split(hostname)[1], body=request.body, headers=dict(request.headers))
    else:
        conn.request(request.command, request.path.split(hostname)[1], headers=dict(request.headers))

    if VERBOSE: logging.debug("State: %s, response: %s" % (conn._HTTPConnection__state, conn._HTTPConnection__response))
    res = conn.getresponse()
    # Response is marked as not being chunked to allow us to send the chunks
    # correctly later on
    res.chunked = False
    if VERBOSE: logging.debug("Got response from server, about to read the content from it")
    res_content = read_response_content(res)
    # TODO: Flash(?) games don't seem to load properly. (Go to neopets->Game Room, and click on any game.)

    if res.getheader('connection', '') == 'close':
        if VERBOSE: logging.debug("Read response content, about to release the server connection (close)")
        release_server_connection(hostname, conn, TTL, True)
    else:
        if VERBOSE: logging.debug("Read response content, about to release the server connection (keep-alive)")
        release_server_connection(hostname, conn, TTL)

    return res_content


if __name__ == "__main__":
    main()
