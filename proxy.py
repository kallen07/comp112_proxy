# code to test rate limiting

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

# associate all client connections to a server connections
server_by_client = bidict()  # key is the client connection

# map client connections to client IPs
client_IPS = dict()

# map in-use HTTP server connections to (TTL, host)
HTTP_conn_to_TTL = dict()

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

# Rate and capacity units: bytes per second (per client)
RATE = 1000000
CAPACITY = 50000000

# Choosing the verbose option prints every debug statement. Otherwise,
# only major ones are printed
VERBOSE = False

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s.%(msecs)03d] (%(threadName)-9s) %(message)s',
                    datefmt='%m-%d,%H:%M:%S')

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
    spawn_helper_thread()

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
                # read from the socket
                try:
                    data = s.recv(1024) # TODO add rate limiting here
                except error, v:
                    data = None
                    logging.debug( "********** Caught exception %s in main" % (str(sys.exc_info())))
                    if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))

                # determine the connection type
                type_of_connection = None
                if s in HTTP_conn_to_TTL:
                    type_of_connection = "HTTP"
                else:
                    type_of_connection = "HTTPS"

                # handle the data
                if data:
                    forward_bytes(s, data)
                # cleanup
                else:
                    logging.debug("closing HTTPS connections")
                    print_server_by_client_bidict()

                    # remove client-server connection pair from server_by_client
                    if s in server_by_client:
                        client_conn = s
                        # s is a client connection, s_prime is a server connection
                        server_conn = server_by_client.get(client_conn)
                        server_by_client.pop(client_conn)
                    else:
                        server_conn = s
                        # s in a server connection, s_prime is a client connection
                        client_conn = server_by_client.inv[server_conn]
                        server_by_client.pop(client_conn)

                    # Stop listening for input on the connections
                    inputs.remove(client_conn)
                    inputs.remove(server_conn)

                    # Close the connections
                    if type_of_connection == "HTTPS":
                        # always close HTTPS server connections
                        server_conn.close()
                    else:
                        # close HTTP server connections unless client exited gracefully
                        close_server_conn = True
                        if s is client_conn and was_clean_close(client_conn):
                            close_server_conn = False
                        TTL, host =  HTTP_conn_to_TTL[server_conn]
                        release_HTTP_server_connection(host, server_conn, TTL, close_server_conn)

                    client_conn.close()

                    # Don't try to read from the connection that we just closed
                    # This fixes the bad file descriptor errors & cascading issues with attempting to remove
                    # non-existent items from server_by_client
                    closed_sockets.append(client_conn)
                    closed_sockets.append(server_conn)



###############################################################################
#                                   UTILS                                     #
###############################################################################

def handle_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="print all debug messages", action="store_true")
    parser.add_argument("-r", "--reuse_connections", help="reuse connections where possible", action="store_true")
    parser.add_argument("--bandwidth", help="limits client's allowed bandwidth (bytes/sec)", type=int, required=False)
    parser.add_argument("--burst_rate", help="max bandwidth client is allowed (bytes/sec)", type=int, required=False)
    parser.add_argument("-a", "--server_address", help="server's address (eg: localhost)", type=str, default="localhost")
    parser.add_argument("port", help="port to run on", type=int)
    args = parser.parse_args()

    global VERBOSE
    if args.verbose:
        VERBOSE = True

    global REUSE_HTTP_CONNECTIONS
    if args.reuse_connections:
        REUSE_HTTP_CONNECTIONS = True

    global RATE
    if args.bandwidth:
        RATE = args.bandwidth

    global CAPACITY 
    if args.burst_rate:
        CAPACITY = args.burst_rate

    global limiter
    storage = token_bucket.MemoryStorage()
    limiter = token_bucket.Limiter(RATE, CAPACITY, storage)

    return args



def create_master_socket(args):
    msock = socket(AF_INET, SOCK_STREAM)
    # TODO: Test server_address arg with non-localhost option
    server_address = (args.server_address, args.port)

    logging.debug('starting up on %s port %s (%s, %s), rate=%d, burst=%d' % 
        (server_address[0], server_address[1],
        "verbose logging" if VERBOSE else "regular (nonverbose) logging",
        "reusing connections" if REUSE_HTTP_CONNECTIONS else
        "using new connections each request",
        RATE, CAPACITY))

    msock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    msock.bind(server_address)
    msock.listen(1)

    return msock


# spawn thread that periodically clears out the HTTP_servers dict
# which allows us to reuse HTTP connections
def spawn_helper_thread():
    if REUSE_HTTP_CONNECTIONS:
        t = Thread(target = prune_HTTP_servers_dict, args = [])
        t.setDaemon(True)
        t.start()


def prune_HTTP_servers_dict():
    while(True):
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
#                           SETUP NEW CONNECTIONS                             #
###############################################################################

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
        # TODO add rate limiting
        data = connection.recv(MAX_HEADER_BYTES)
        request = HTTPRequest(data)
        if VERBOSE: logging.debug('received "%s"' % data)
        if data:
            # save client IP address
            client_IPS[connection] = client_address[0]
            if request.command == "CONNECT":
                # the new conncetion is an HTTPS CONNECT request
                server_conn = setup_HTTPS_connection(connection, request)
            else:
                # the connection is an HTTP request
                server_conn = setup_HTTP_connection(connection, request, data)
            return [connection, server_conn]

    except error, v:
        # don't print timed out exceptions since they are an unpreventable error
        if v[0] !=  "timed out":
            logging.debug( "********** Caught exception: %s" % (str(sys.exc_info())))
            if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))
        # if we can't read from the new socket or we can't write to the new socket
        # something is very wrong, close and return
        connection.close()
        return None


# returns (hostname, port)
# where port could be None
def split_host(host):
    try:
        hostname, port = host.split(":")  # this could be fragile
        port = int(port)
        return (hostname, port)
    except:
        return (host, None)


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
    hostname, port = split_host(request.headers["host"])
    if port is None:
        port = 443
    server_conn.connect((hostname, int(port)))

    # map between client and server connections
    server_by_client.put(client_conn, server_conn)
    return server_conn


def setup_HTTP_connection(client_conn, request, raw_data):
    host = request.headers["host"]

    # acquire HTTP server connection
    if VERBOSE: logging.debug("setting up an HTTP connection to server with hostname %s" % host)
    server_conn, TTL = acquire_HTTP_server_connection(host)
    HTTP_conn_to_TTL[server_conn] = (TTL, host)
    if VERBOSE: logging.debug("Using connection %s (TTL: %s)" % (server_conn, TTL))
    
    # debugging
    logging.debug(request.headers)
    if "Proxy-Connection" in request.headers:
        if VERBOSE: logging.debug("proxy-connection was in the headers, with value %s" % request.headers["Proxy-Connection"])
        #request.headers['Connection'] = request.headers['Proxy-Connection']
        # logging.debug("Added Connection header to request head")
    if VERBOSE: logging.debug("Request path: %s" % request.path)

    # map between client and server connections
    server_by_client.put(client_conn, server_conn)

    # send request to server
    # send_HTTP_request_to_server(server_conn, request)
    forward_bytes(src_conn = client_conn, data = raw_data)

    return server_conn


###############################################################################
#                              HANDLE REQUESTS                                #
###############################################################################

def forward_bytes(src_conn, data):
    try:
        if src_conn in server_by_client:
            # data is from client
            server = server_by_client.get(src_conn)
            if VERBOSE: logging.debug("Writing %d bytes to the server with fd %d" %
                                      (len(data), server.fileno()))
            server.sendall(data)
        else:
            # data is from server
            client = server_by_client.inv[src_conn]
            if VERBOSE: logging.debug("Writing %d bytes to the client with fd %d" %
                                      (len(data), client.fileno()))
            client.sendall(data)
            return
    except error, v:
        logging.debug( "********** Caught exception: %s" % (str(sys.exc_info())))
        if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))


def send_HTTP_request_to_server(server_conn, request):
    pass;
    # TODO IMPLEMENT THIS

    # headers = response.getheaders()
    # http_version = '1.0' if response.version is 10 else '1.1'

    # formatted_header = 'HTTP/{} {} {}\r\n{}\r\n\r\n'.format(
    #     http_version, str(response.status), response.reason,
    #     '\r\n'.join('{}: {}'.format(k, v) for k, v in headers)
    # )
    # if VERBOSE: logging.debug("Sending headers as soon as we get the bandwidth")
    # assert_bandwidth_available(client_address[0], len(formatted_header))
    # client_connection.sendall(formatted_header)

    # if VERBOSE: logging.debug(formatted_header)


def handle_HTTP_request(connection, client_address, request):
    # Token bucket will automatically begin with CAPACITY tokens. We want
    # it to begin at 0 tokens
    # tokens == 0 if the current user has not yet made a request
    if limiter._storage.get_token_count(client_address[0]) == 0:
        logging.debug("%s is accessing the proxy for the first time, their token count is 0" % (str(client_address)))
        # Replenish with capacity = 0 to force the initial token count to be 0
        limiter._storage.replenish(client_address[0], RATE, 0) 
        if limiter._storage.get_token_count(client_address[0]) != 0:
            logging.debug("ERROR: Token count should be 0 because we replenished with 0, but it's actually %f" %
                (limiter._storage.get_token_count(client_address[0])))
    else:
        logging.debug("%s has %f tokens" % (str(client_address), limiter._storage.get_token_count(client_address[0])))

    # handle first request
    get_and_forward_response_from_server(request, client_address, connection)

    # if client sends another request, then handle it
    while True:
        try:
            if VERBOSE: logging.debug('=================== reading from %s ===================' % str(client_address))
            else: logging.debug("Reading from %s" % str(client_address))

            data = connection.recv(MAX_HEADER_BYTES)
            request = HTTPRequest(data)
            if VERBOSE: logging.debug('received "%s"' % data)
            if data:
                if request.command == "CONNECT":
                    handle_HTTPS_request(connection, request)
                    break
                else:
                    get_and_forward_response_from_server(request, client_address, connection)
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
                logging.debug("!!!!bad status line...")
                #raise
            logging.debug('=================== CLOSING SOCKET ==================')
            connection.close()
            break



###############################################################################
#                          RATE LIMITING UTILS                                #
###############################################################################

def assert_bandwidth_available(client_ip, bytes_requested):
    success = limiter.consume(client_ip, bytes_requested)
    while not success:
        # Seconds to wait until bytes_requested tokens are expected to be available
        # Note: They actually do not replenish at the exact time we expect, so for small reads we will
        # wait longer to allow other threads to run. Double the wait time if it's for 1
        # byte only
        time_to_wait = bytes_requested / float(RATE)
        if bytes_requested <= 1:
            time_to_wait *= 2
            
        logging.debug("Client %s is being rate limited for %f seconds to consume %d tokens" % (client_ip, time_to_wait, bytes_requested))
        time.sleep(time_to_wait)
        # Try to get tokens again
        success = limiter.consume(client_ip, bytes_requested)
    logging.debug("Successfully consumed the %d tokens requested" % bytes_requested)

def get_max_bandwidth(client_ip, bytes_wanted):
    global limiter
    curr_available = limiter._storage.get_token_count(client_ip)
    logging.debug("Getting min of (%f and %f - curr avail)" % (bytes_wanted, curr_available))

    return min(bytes_wanted, curr_available)


###############################################################################
#                      HTTP DATA STRUCTURE MANIPULATION                       #
###############################################################################

def was_clean_close(client_conn):
    return False # TODO fix this

def acquire_HTTP_server_connection(host):
    # look for an existing, available connection
    if REUSE_HTTP_CONNECTIONS:
        with servers_lock:
            connections = HTTP_servers.pop(host, None)
            if connections is not None:
                conn = connections.pop()
                if connections:
                    HTTP_servers[host] = connections
                return conn

    # setup a new HTTP connection
    server_conn = socket(AF_INET, SOCK_STREAM)
    hostname, port = split_host(host)
    if port is None:
        port = 80
    server_conn.connect((hostname, port))
    return (server_conn, datetime.now())

def release_HTTP_server_connection(host, conn, TTL, connection_close=False):
    if REUSE_HTTP_CONNECTIONS and not connection_close:
        # increase time to keep the connection open
        TTL = TTL + timedelta(seconds=5)
        if VERBOSE: logging.debug("Extending TTL for connection %s (Host: %s) to %s" % (conn, host, TTL))
        with servers_lock:
            if host in HTTP_servers:
                if VERBOSE: logging.debug("There were already %d connections for this host" % (len(HTTP_servers[host])))
                HTTP_servers[host].append((conn, TTL))
            else:
                if VERBOSE: logging.debug("This is the only connection right now for this host")
                HTTP_servers[host] = [(conn, TTL)]
    else:
        logging.debug("Closing connection %s (host: %s)" % (conn, host))
        conn.close()



###############################################################################
#                               DEBUG FUNCTIONS                               #
###############################################################################

def print_server_by_client_bidict():
    socketfds = []
    for key in server_by_client.keys():
        socketfds.append((key.fileno(), server_by_client.get(key).fileno()))
    socketfds.sort(key=lambda tup: tup[1])
    logging.debug("printing the server_by_client object, which has %d items..." %(len(socketfds)))
    logging.debug(socketfds)


if __name__ == "__main__":
    main()
