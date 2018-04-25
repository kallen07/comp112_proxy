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

# map client connections to bookkeeping information about the pending request
# key: client_conn
# values: dict containing the following
#     "state" -> "IDLE", "READING_HEADER", "READING_DATA", "READING_CHUNK",
#                 or "READING_CHUNK_SIZE"
#     "method" -> The method of the ongoing request (needed to interpret the
#                 eventual response)
#     "buffered_header" -> What has been read of the header so far
#     "curr_content_length" -> The size of the next piece of data we will be
#                              reading. This is either Content-Length from the
#                              header or the current chunk size
#     "bytes_sent" -> The number of bytes sent for the current piece of data
#     "buffered_chunk_size_str" -> What has been read so far of the chunk size
#                                  hex string
HTTP_client_req_info = dict()

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

    inputs = set([msock])

    global HTTP_client_req_info

    while inputs:
        if VERBOSE: logging.debug("Num inputs: %d \n%s" % (len(inputs), str(inputs)))
        readable, _, _ = select.select(inputs, [], [])
        if VERBOSE: logging.debug("Num readable: %d" % len(readable))

        closed_sockets = []

        for s in readable:
            if s is msock:
                new_connections = accept_new_connection(s)
                logging.debug("Accepted new connection: %s" % str(new_connections))
                if new_connections:
                    for connection in new_connections:
                        inputs.add(connection)
            elif s not in closed_sockets:
                # determine if the socket we read from was a client or a server
                if s in server_by_client:
                    client_conn = s
                    server_conn = server_by_client.get(client_conn)
                else:
                    server_conn = s
                    client_conn = server_by_client.inv[server_conn]

                # determine the connection type
                type_of_connection = None
                if server_conn in HTTP_conn_to_TTL:
                    type_of_connection = "HTTP"
                else:
                    type_of_connection = "HTTPS"

                logging.debug("%s was a %s %s" % (str(s), type_of_connection, "client" if s == client_conn else "server"))

                # read from the socket
                try:
                    bytes_to_read = MAX_HEADER_BYTES
                    # TODO add rate limiting here
                    if type_of_connection == "HTTP" and s == client_conn:
                        # We peek here in case the client is still waiting to finish
                        # the previous response
                        data = s.recv(bytes_to_read, MSG_PEEK) 
                        if data:
                            if expecting_response_for_client(client_conn):
                                if VERBOSE: logging.debug("******* Ignoring client %s (peeked %d bytes)" % (str(client_conn), len(data)))
                                continue
                            else:
                                if VERBOSE: logging.debug("******* Reading new request from client %s (read %d bytes)" % (
                                    str(client_conn), len(data)))
                                data = s.recv(bytes_to_read) 
                    else:
                        data = s.recv(bytes_to_read)
                        logging.debug("******* Read %d bytes from socket %s, which is an %s %s" %
                            (len(data), str(s), type_of_connection, "client" if s == client_conn else "server"))
                except error, v:
                    data = None
                    logging.debug( "********** Caught exception %s in main" % (str(sys.exc_info())))
                    if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))

                # tODO: determine how many tokens were actually used and replenish what
                # wasn't used

                # TODO: Wrap this whole thing in a try-catch and close the connection if
                # an exception gets thrown

                # handle the data
                if data:
                    # We read a response from an HTTP server connection
                    if type_of_connection == "HTTP" and s == server_conn:
                        handle_HTTP_server_response(client_conn, server_conn, data)
                    else:
                        forward_bytes(s, data)

                        if type_of_connection == "HTTP" and s == client_conn:
                            # we read a new request from the client
                            request = HTTPRequest(data)
                            if VERBOSE: logging.debug("Received new request from http client %s, updating state now to READING_HEADER" % str(client_conn))
                            if VERBOSE: logging.debug("Request body was: %s" % data)
                            reset_client_req_info(client_conn)
                            HTTP_client_req_info[client_conn]["state"] = "READING_HEADER"
                            HTTP_client_req_info[client_conn]["method"] = request.command
                            
                # cleanup
                # TODO: Acquire and release HTTP server connection *per request*, not per client
                # TODO: Move cleanup code to function, and run it on exception as well
                else:
                    logging.debug("closing connections")
                    print_server_by_client_bidict()

                    # remove client-server connection pair from server_by_client
                    server_by_client.pop(client_conn)

                    # Stop listening for input on the connections
                    inputs.remove(client_conn)
                    inputs.remove(server_conn)

                    if VERBOSE: logging.debug("No longer listening on client %s and server %s" %
                        (str(client_conn), str(server_conn)))
                    if VERBOSE: logging.debug("Just to confirm, this is what inputs looks like now:\n%s" %
                        (str(inputs)))

                    # Close the connections
                    if type_of_connection == "HTTPS":
                        # always close HTTPS server connections
                        server_conn.close()
                    else:
                        # close HTTP server connections unless client exited gracefully
                        close_server_conn = True
                        if s is client_conn and not expecting_response_for_client(client_conn):
                            close_server_conn = False
                        release_HTTP_server_connection(server_conn, close_server_conn)
                        del HTTP_client_req_info[client_conn]

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
    client_conn, client_address = msock.accept()
    logging.debug('=================== new connection from %s ===================' % str(client_address))
    client_conn.settimeout(0.5) # TODO should this be less?

    try:
        # read the first request and determine the protocol (HTTP or HTTPS)
        # TODO add rate limiting
        data = client_conn.recv(MAX_HEADER_BYTES)
        request = HTTPRequest(data)
        if VERBOSE: logging.debug('received "%s"' % data)
        if data:
            # save client IP address
            client_IPS[client_conn] = client_address[0]
            if request.command == "CONNECT":
                # the new conncetion is an HTTPS CONNECT request
                server_conn = setup_HTTPS_connection(client_conn, request)
            else:
                # the connection is an HTTP request
                HTTP_client_req_info[client_conn] = dict()
                reset_client_req_info(client_conn)
                HTTP_client_req_info[client_conn]["state"] = "READING_HEADER"
                HTTP_client_req_info[client_conn]["method"] = request.command

                server_conn = setup_HTTP_connection(client_conn, request, data)
                if VERBOSE: logging.debug("Set up http connection: client %s server %s" % (
                    str(client_conn), str(server_conn)))
            return [client_conn, server_conn]

    except error, v:
        # don't print timed out exceptions since they are an unpreventable error
        if v[0] !=  "timed out":
            logging.debug( "********** Caught exception: %s" % (str(sys.exc_info())))
            if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))
        # if we can't read from the new socket or we can't write to the new socket
        # something is very wrong, close and return
        client_conn.close()
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
    if VERBOSE: logging.debug("Request (raw data):\n%s" % raw_data)
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
#                         HANDLE REQUESTS / RESPONSES                         #
###############################################################################

def forward_bytes(src_conn, data):
    try:
        if src_conn in server_by_client:
            # data is from client
            server = server_by_client.get(src_conn)
            if VERBOSE: logging.debug("Writing %d bytes to the server %s with fd %d (first 30: %s)" %
                                      (len(data), str(server), server.fileno(), data[:30]))
            if "Cannot" in data[:30]:
                if VERBOSE: logging.debug("Full data:\n%s" % data)
            server.sendall(data)
        else:
            # data is from server
            client = server_by_client.inv[src_conn]
            if VERBOSE: logging.debug("Writing %d bytes to the client %s with fd %d (first 30: %s)" %
                                      (len(data), str(client), client.fileno(), data[:30]))
            if "Cannot" in data[:30]:
                if VERBOSE: logging.debug("Full data:\n%s" % data)
            client.sendall(data)
    except error, v:
        logging.debug( "********** Caught exception: %s" % (str(sys.exc_info())))
        if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))

def setup_next_state_from_header(client_conn):
    buffered_header = HTTP_client_req_info[client_conn]["buffered_header"]
    method = HTTP_client_req_info[client_conn]["method"]

    if VERBOSE: logging.debug("[%s] Buffered header: |%s|" % (str(client_conn), buffered_header))
    response = httplib.HTTPMessage(StringIO(buffered_header), 0)
    response.readheaders()
    content_length = response.getheader("content-length")
    transfer_encoding = response.getheader("transfer-encoding") 

    reset_client_req_info(client_conn)

    if transfer_encoding and transfer_encoding.lower() == "chunked":
        HTTP_client_req_info[client_conn]["state"] = "READING_CHUNK_SIZE"
        return "READING_CHUNK_SIZE"
    elif content_length:
        HTTP_client_req_info[client_conn]["state"] = "READING_DATA"
        HTTP_client_req_info[client_conn]["curr_content_length"] = int(content_length)
        HTTP_client_req_info[client_conn]["bytes_sent"] = 0
        return "READING_DATA"
    else:
        # The response must not have included a body (https://tools.ietf.org/html/rfc2616#section-4.4)
        status_line = buffered_header[:buffered_header.find("\n")]
        try:
            [version, status] = status_line.split(None, 1)
        except ValueError:
            # Malformed response - no whitespace
            return "ERROR: Malformed status line: %s" % status_line


        status_code = status_line.split(None)[0]

        if (status_code == 204 or status_code == 304 or
            100 <= status_code < 200 or      # 1xx codes
            method == "HEAD"):
            if VERBOSE: logging.debug("[%s] Received a response that does not have a body, so going back to idle"  % str(client_conn))
            reset_client_req_info(client_conn)
            return "IDLE"
        return "ERROR: Status line was fine, and code was not one where it should have no body. Status line: %s" % status_line
    
def forward_chunk_to_client(client_conn, server_conn, data):
    content_length = int(HTTP_client_req_info[client_conn]["curr_content_length"])
    bytes_sent = int(HTTP_client_req_info[client_conn]["bytes_sent"])

    # Each chunk is terminated by a two-byte \r\n. We manually add on these two
    # bytes here to determine how many bytes to actually send
    bytes_to_send = min(content_length + 2 - bytes_sent, len(data))

    if bytes_to_send > 0:
        forward_bytes(server_conn, data[:bytes_to_send])

    bytes_sent += bytes_to_send
    HTTP_client_req_info[client_conn]["bytes_sent"] = bytes_sent

    # If we've sent all the data plus the terminating \r\n...
    if bytes_sent == content_length + 2:
        reset_client_req_info(client_conn)
        # If it was a nonterminating chunk, prepare to read the next chunk size
        if content_length > 0:
            HTTP_client_req_info[client_conn]["bytes_sent"] = 0
            HTTP_client_req_info[client_conn]["state"] = "READING_CHUNK_SIZE"
            if VERBOSE: logging.debug("[%s] Done sending chunk, req info is %s" % (str(client_conn), str(HTTP_client_req_info[client_conn])))
        # If it was a terminating chunk, we're done reading this response
        else:
            if VERBOSE: logging.debug("[%s] Done sending terminating chunk, req info is %s" % (str(client_conn), str(HTTP_client_req_info[client_conn])))

    return data[bytes_to_send:]

def forward_data_to_client(client_conn, server_conn, data):
    content_length = int(HTTP_client_req_info[client_conn]["curr_content_length"])
    bytes_sent = int(HTTP_client_req_info[client_conn]["bytes_sent"])
    state = HTTP_client_req_info[client_conn]["state"]
    if VERBOSE: logging.debug("[%s] Forwarding data to client (server %s). Content length: %s, bytes_sent: %s, state: %s" % (
        str(client_conn), str(server_conn), content_length, str(bytes_sent), state))

    bytes_to_send = min(content_length - bytes_sent, len(data))
    forward_bytes(server_conn, data[:bytes_to_send])

    bytes_sent += bytes_to_send
    HTTP_client_req_info[client_conn]["bytes_sent"] = bytes_sent

    if bytes_sent == content_length:
        reset_client_req_info(client_conn)
        if VERBOSE: logging.debug("[%s] Bytes_sent == content_length, state is now %s" % (
            str(client_conn), HTTP_client_req_info[client_conn]["state"]))
    
    return data[bytes_to_send:]

def reset_client_req_info(client_conn):
    logging.debug("Resetting client info for %s" % str(client_conn))
    HTTP_client_req_info[client_conn]["method"] = ""
    HTTP_client_req_info[client_conn]["bytes_sent"] = 0
    HTTP_client_req_info[client_conn]["content_length"] = 0
    HTTP_client_req_info[client_conn]["state"] = "IDLE"
    HTTP_client_req_info[client_conn]["buffered_chunk_size_str"] = ""
    HTTP_client_req_info[client_conn]["buffered_header"] = ""

def update_buffered_header(client_conn, server_conn, data):
    buffered_header = HTTP_client_req_info[client_conn]["buffered_header"]
    if VERBOSE: logging.debug("Updating buffered header for client %s. Header is %d bytes long so far (last 10: %s)" % 
        (str(client_conn), len(buffered_header), buffered_header[-10:]))

    # Add the bytes to a list to avoid creating a new string each time we want
    # to append one byte. This starts with the last three bytes from the buffered
    # header so we can always just check the last three elements of this list
    bytes_list = list(buffered_header[-3:])

    # Determine how many bytes we are repeating. These are bytes that should be
    # skipped when we construct the new buffered header
    repeated_bytes = len(bytes_list)

    curr_pos_in_data = 0

    # Add one byte at a time to the buffered header until we run out of data
    # or encounter the end of the header (empty line with carriage return)
    while "".join(bytes_list[-3:]) != b"\n\r\n":
        # out of data to read
        if curr_pos_in_data >= len(data):
            # add on the the bytes we read
            HTTP_client_req_info[client_conn]["buffered_header"] = buffered_header + "".join(bytes_list[repeated_bytes:])
            if VERBOSE: logging.debug("[%s] Ran out of data to read while adding to header. Header is %d bytes long after adding on all we could" %
                (str(client_conn), len(HTTP_client_req_info[client_conn]["buffered_header"])))
            return ""

        bytes_list += data[curr_pos_in_data:curr_pos_in_data + 1]
        curr_pos_in_data += 1
        
    buffered_header = buffered_header + "".join(bytes_list[repeated_bytes:])
    HTTP_client_req_info[client_conn]["buffered_header"] = buffered_header
    if VERBOSE: logging.debug("[%s] Done reading header, curr_pos_in_data was %d. Last 10 bytes are now: %s" % 
        (str(client_conn), curr_pos_in_data, buffered_header[-10:]))
    
    forward_bytes(server_conn, buffered_header)

    next_state = setup_next_state_from_header(client_conn)
    if VERBOSE: logging.debug("[%s] Determined that the next state should be %s" % (str(client_conn), next_state))

    return data[curr_pos_in_data:]


def update_buffered_chunk_size_str(client_conn, server_conn, data):
    size_str = HTTP_client_req_info[client_conn]["buffered_chunk_size_str"]

    # Add the bytes to a list to avoid creating a new string each time we want
    # to append one byte. This starts with the last two bytes from the buffered
    # header so we can always just check the last two elements of this list
    bytes_list = list(size_str[-2:])

    # Determine how many bytes we are repeating. These are bytes that should be
    # skipped when we construct the new size_str
    repeated_bytes = len(bytes_list)

    curr_pos_in_data = 0

    # Add one byte at a time to the buffered header until we run out of data
    # or encounter the end of the header (empty line with carriage return)
    while "".join(bytes_list[-2:]) != b"\r\n":
        # out of data to read
        if curr_pos_in_data >= len(data):
            # add on the the bytes we read
            HTTP_client_req_info[client_conn]["buffered_chunk_size_str"] = size_str + "".join(bytes_list[repeated_bytes:])
            if VERBOSE: logging.debug("[%s] Ran out of data to read while adding to size_str. Size str is now |%s| (%d bytes long)" %
                (str(client_conn), HTTP_client_req_info[client_conn]["buffered_chunk_size_str"], 
                    len(HTTP_client_req_info[client_conn]["buffered_chunk_size_str"])))
            return ""


        bytes_list += data[curr_pos_in_data:curr_pos_in_data + 1]
        curr_pos_in_data += 1
        

    size_str = size_str + "".join(bytes_list[repeated_bytes:])
    if VERBOSE: logging.debug("[%s] Forwarding along size_str of %d bytes: %s" % (str(client_conn), len(size_str), size_str))
    forward_bytes(server_conn, size_str)

    # Remove chunk extensions (optional data after the chunk size, preceeded by ;)
    if ";" in size_str:
        size_str = size_str[:find(";")]
    chunk_size = int(size_str[:-2], 16)
    if VERBOSE: logging.debug("[%s] Parsed out chunk (%s) size: %d" % (str(client_conn), size_str[:-2], chunk_size))

    HTTP_client_req_info[client_conn]["curr_content_length"] = chunk_size
    HTTP_client_req_info[client_conn]["bytes_sent"] = 0
    HTTP_client_req_info[client_conn]["state"] = "READING_CHUNK"

    return data[curr_pos_in_data:]

def handle_HTTP_server_response(client_conn, server_conn, data):
    state = HTTP_client_req_info[client_conn]["state"]
    if VERBOSE: logging.debug("----------------------")
    if VERBOSE: logging.debug("Handling %d bytes of data for client %s, state is %s, req info: %s" % (len(data),
        str(client_conn), state, HTTP_client_req_info[client_conn]))

    unread_data = ""
    if state == "IDLE":
        # This is an error case. The server should not be sending data while
        # the connection is in idle state
        logging.debug("ERROR: Server (%s) for client %s sent data while in idle state. Data read: %s" %
            (str(server_conn), str(client_conn), data))
    elif state == "READING_HEADER":
        unread_data = update_buffered_header(client_conn, server_conn, data)
    elif state == "READING_DATA":
        unread_data = forward_data_to_client(client_conn, server_conn, data)
    elif state == "READING_CHUNK":
        unread_data = forward_chunk_to_client(client_conn, server_conn, data)
    elif state == "READING_CHUNK_SIZE":
        unread_data = update_buffered_chunk_size_str(client_conn, server_conn, data)
    else: 
        logging.debug("ERROR: Unknown state %s" % state)

    if unread_data:
        if VERBOSE: logging.debug("[%s] Some data left unread after handling. Calling handle again" % str(client_conn))
        handle_HTTP_server_response(client_conn, server_conn, unread_data)

###############################################################################
#                          RATE LIMITING UTILS                                #
###############################################################################

def get_max_bandwidth(client_ip, bytes_wanted):
    global limiter
    curr_available = limiter._storage.get_token_count(client_ip)
    if VERBOSE: logging.debug("Getting min of (%f and %f - curr avail)" % (bytes_wanted, curr_available))

    return min(bytes_wanted, curr_available)

def put_back_tokens(client_ip, num_tokens):
    # TODO: implement
    pass

###############################################################################
#                      HTTP DATA STRUCTURE MANIPULATION                       #
###############################################################################

def expecting_response_for_client(client_conn):
    # A request is pending if the request state is anything other
    # than IDLE
    return HTTP_client_req_info[client_conn]["state"] != "IDLE" 

def was_clean_close(client_conn):
    # A client close is only considered "clean" if there is no pending
    # data from the server associated with this client. This means the
    # request state must be "IDLE"
    return HTTP_client_req_info[client_conn]["state"] == "IDLE"

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

def release_HTTP_server_connection(server_conn, connection_close=False):
    TTL, host =  HTTP_conn_to_TTL[server_conn]
    if REUSE_HTTP_CONNECTIONS and not connection_close:
        # increase time to keep the connection open
        TTL = TTL + timedelta(seconds=5)
        if VERBOSE: logging.debug("Extending TTL for connection %s (Host: %s) to %s" % (server_conn, host, TTL))
        with servers_lock:
            if host in HTTP_servers:
                if VERBOSE: logging.debug("There were already %d connections for this host" % (len(HTTP_servers[host])))
                HTTP_servers[host].append((server_conn, TTL))
            else:
                if VERBOSE: logging.debug("This is the only connection right now for this host")
                HTTP_servers[host] = [(server_conn, TTL)]
    else:
        logging.debug("Closing connection %s (host: %s)" % (server_conn, host))
        server_conn.close()

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

def print_HTTP_client_req_info():
    client_conns = [str(key) for key in HTTP_client_req_info.keys()]

    logging.debug("printing the server_by_client object, which has %d items..." %(len(client_conns)))
    logging.debug(client_conns)

if __name__ == "__main__":
    main()
