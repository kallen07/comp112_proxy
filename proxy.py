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
import re

###############################################################################
#                          CONSTANTS & GLOBALS                                #
###############################################################################

# max number of bytes that can be in an HTTP header
# TODO: Can this cover requests with bodies? (eg POST)
MAX_HEADER_BYTES = 8000

# associate all client connections to a server connection
server_by_client = bidict()  # key is the client connection

# map client connections to client IPs
client_IPS = dict()

# List of HTTP clients and servers
HTTP_connections = set()

# determine if we are using a blacklist
USING_BLACKLIST = False
# list of blacklisted hostnames
blacklisted_hostnames = list()

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


# rate limiting mechanism (token bucket algo)
# rate: Number of tokens per second to add to the
#   bucket. Over time, the number of tokens that can be
#   consumed is limited by this rate. Each token represents
#   some percentage of a finite resource that may be
#   utilized by a consumer.
# capacity: Maximum number of tokens that the bucket
#   can hold. Once the bucket is full, additional tokens
#   are discarded.

# Rate limiting
USING_RATE_LIMITING = False 
# Rate and capacity units: bytes per second (per client)
RATE = None
CAPACITY = None

# Choosing the verbose option prints every debug statement. Otherwise,
# only major ones are printed
VERBOSE = False

# Control whether we are replacing content with "Fahad"
MODIFY_CONTENT = False
CONTENT_REPLACE_TARGET = "" 

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

    inputs = set([msock])

    global HTTP_client_req_info

    while inputs:
        readable, _, _ = select.select(inputs, [], [])

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
                if s in HTTP_connections:
                    type_of_connection = "HTTP"
                else:
                    type_of_connection = "HTTPS"

                if VERBOSE: logging.debug("%s was a %s %s" % (str(s), type_of_connection, "client" if s == client_conn else "server"))

                # read from the socket
                try:
                    # We rate limit only on download, so we limit the amount of data the client
                    # can get from the server at a time
                    if USING_RATE_LIMITING and s == server_conn:
                        bytes_to_read = int(get_max_bandwidth(client_conn, MAX_HEADER_BYTES))
                    else:
                        bytes_to_read = MAX_HEADER_BYTES

                    # Don't read if we cannot read any bytes, or this will be interpreted as the connection
                    # closing
                    if bytes_to_read == 0:
                        continue

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
                        if VERBOSE: logging.debug("******* Read %d bytes from socket %s, which is an %s %s" %
                            (len(data), str(s), type_of_connection, "client" if s == client_conn else "server"))

                except error, v:
                    data = None
                    logging.debug( "********** Caught exception %s in main" % (str(sys.exc_info())))
                    if VERBOSE: logging.debug(traceback.print_tb(sys.exc_info()[2]))

                if USING_RATE_LIMITING and s == server_conn and len(data) > 0:
                    # Remove tokens based on how much was actually read
                    limiter.consume(client_IPS[client_conn], len(data))

                # handle the data
                if data:
                    # We read a response from an HTTP server connection
                    if type_of_connection == "HTTP" and s == server_conn:
                        handle_HTTP_server_response(client_conn, server_conn, data)
                    else:
                        if type_of_connection == "HTTP" and s == client_conn:
                            if MODIFY_CONTENT:
                                data = remove_accept_encoding(data)
                            # we read a new request from the client
                            request = HTTPRequest(data)
                            
                            if VERBOSE: logging.debug("Received new request from http client %s, updating state now to READING_HEADER" % str(client_conn))
                            if VERBOSE: logging.debug("Request body was: %s" % data)
                            reset_client_req_info(client_conn)
                            HTTP_client_req_info[client_conn]["state"] = "READING_HEADER"
                            HTTP_client_req_info[client_conn]["method"] = request.command

                        forward_bytes(s, data)       
                            
                # cleanup
                else:
                    logging.debug("closing connections")
                    print_server_by_client_bidict()

                    # remove client-server connection pair from server_by_client
                    server_by_client.pop(client_conn)

                    # remove client and server from list of HTTP clients
                    if client_conn in HTTP_connections:
                        HTTP_connections.remove(client_conn)
                    if server_conn in HTTP_connections:
                        HTTP_connections.remove(server_conn)

                    # Stop listening for input on the connections
                    inputs.remove(client_conn)
                    inputs.remove(server_conn)

                    # Close the connections
                    server_conn.close()
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
    parser.add_argument("--fahad_keyword", help="five-letter word to replace with 'Fahad'", type=str, default="their")
    parser.add_argument("--bandwidth", help="limits client's allowed bandwidth (bytes/sec)", type=int, required=False)
    parser.add_argument("--burst_rate", help="max bandwidth client is allowed (bytes/sec)", type=int, required=False)
    parser.add_argument("-a", "--server_address", help="server's address (eg: localhost)", type=str, default="localhost")
    parser.add_argument("-b", "--blacklist_sites", help="name of file containing list of blacklisted hostnames", type=str, default=None)
    parser.add_argument("port", help="port to run on", type=int)
    args = parser.parse_args()

    global VERBOSE
    if args.verbose:
        VERBOSE = True

    global RATE
    if args.bandwidth:
        RATE = args.bandwidth

    global CAPACITY
    if args.burst_rate:
        CAPACITY = args.burst_rate

    global USING_RATE_LIMITING
    if args.burst_rate and args.bandwidth:
        USING_RATE_LIMITING = True

    global MODIFY_CONTENT
    global CONTENT_REPLACE_TARGET
    if args.fahad_keyword:
        MODIFY_CONTENT = True
        CONTENT_REPLACE_TARGET = args.fahad_keyword[:5] # limit to 5 letters

    global limiter
    global storage
    if USING_RATE_LIMITING:
        storage = token_bucket.MemoryStorage()
        limiter = token_bucket.Limiter(RATE, CAPACITY, storage)

    global USING_BLACKLIST
    if args.blacklist_sites is not None:
        USING_BLACKLIST = True
        parse_blacklisted_hostnames(args.blacklist_sites)

    return args

def parse_blacklisted_hostnames(filename):
    global blacklisted_hostnames
    with open(filename) as f:
        blacklisted_hostnames = f.readlines()
        blacklisted_hostnames = [x.strip() for x in blacklisted_hostnames]

    logging.debug(blacklisted_hostnames)

def create_master_socket(args):
    msock = socket(AF_INET, SOCK_STREAM)
    server_address = (args.server_address, args.port)

    logging.debug('starting up on %s port %s, %s, rate=%s, burst=%s' % 
        (server_address[0], server_address[1],
        "verbose logging" if VERBOSE else "regular (nonverbose) logging",
        RATE, CAPACITY))

    msock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    msock.bind(server_address)
    msock.listen(1)

    return msock


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
        if MODIFY_CONTENT:
            data = remove_accept_encoding(data)

        request = HTTPRequest(data)

        if VERBOSE: logging.debug('received "%s"' % data)
        if data:
            # ignore this client if it is trying to access a blacklisted site
            hostname, _ = split_host(request.headers["host"])
            logging.debug("the hostname is %s", hostname)
            if is_blacklisted(hostname):
                send_blacklisted_response(client_conn, hostname)
                client_conn.close()
                return None

            # save client IP address
            client_IPS[client_conn] = client_address[0]
            if request.command == "CONNECT":
                # the new conncetion is an HTTPS CONNECT request
                server_conn = setup_HTTPS_connection(client_conn, request)
                if VERBOSE: logging.debug("Set up https connection: client %s server %s" % (
                    str(client_conn), str(server_conn)))
            else:
                # the connection is an HTTP request
                HTTP_client_req_info[client_conn] = dict()
                reset_client_req_info(client_conn)
                HTTP_client_req_info[client_conn]["state"] = "READING_HEADER"
                HTTP_client_req_info[client_conn]["method"] = request.command

                server_conn = setup_HTTP_connection(client_conn, request, data)
                if VERBOSE: logging.debug("Set up http connection: client %s server %s" % (
                    str(client_conn), str(server_conn)))

            if USING_RATE_LIMITING:
                set_token_quantity(client_conn, 0)
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
        hostname, port = host.split(":")  # TODO: this could be fragile
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

    # parse hostname and port
    hostname, port = split_host(request.headers["host"])
    if port is None:
        port = 443

    # setup HTTPS connection to server for client
    server_conn = socket(AF_INET, SOCK_STREAM)
    server_conn.connect((hostname, int(port)))

    # map between client and server connections
    server_by_client.put(client_conn, server_conn)
    return server_conn


def setup_HTTP_connection(client_conn, request, raw_data):
    host = request.headers["host"]

    # acquire HTTP server connection
    if VERBOSE: logging.debug("setting up an HTTP connection to server with hostname %s" % host)
    server_conn = open_HTTP_server_connection(host)
    if VERBOSE: logging.debug("Using connection %s" % server_conn)
    
    # debugging
    if VERBOSE: logging.debug("Request (raw data):\n%s" % raw_data)
    if VERBOSE: logging.debug("Request path: %s" % request.path)

    # map between client and server connections
    server_by_client.put(client_conn, server_conn)

    HTTP_connections.add(client_conn)
    HTTP_connections.add(server_conn)

    # send request to server
    forward_bytes(src_conn = client_conn, data = raw_data)

    return server_conn

def is_blacklisted(hostname):
    if USING_BLACKLIST:
        for bad_hostname in blacklisted_hostnames:
            if bad_hostname in hostname:
                logging.debug("not allowing a connection from %s since %s is blacklisted" % (hostname, bad_hostname))
                return True

    return False

def send_blacklisted_response(client_conn, hostname):
    html_body = "<html><head><title>Forbidden</title></head><body> \
    <h1>Forbidden</h1> \
    <p>Uh oh, Kalina and Elena Inc. blacklisted all {} webpages because \
    they impact employee productivity! This attempt has been recorded</p> \
    </body></html>".format(hostname)
    formatted_res = 'HTTP/{} {} {}\r\n{}\r\n\r\n{}\r\n'.format(
        '1.0', 403, "Forbidden",
        'Content-Type: text/html',
        html_body
    )
    if VERBOSE: logging.debug("sending the following blacklisted res: \n %s" % formatted_res)
    client_conn.sendall(formatted_res)


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

def get_info_from_header(buffered_header):
    response = httplib.HTTPMessage(StringIO(buffered_header), 0)
    
    # On OSX you seem to need to read the headers, while on Windows you do not. The two
    # seem to be mutually exclusive - one will not work on the other. This is a workaround
    # to check whether we need to explicitly read the headers or not. If both attempts fail,
    # we manually parse the headers.

    # Note: We use content-type to determine whether the headers have been parsed out
    # correctly by the HTTPMessage, since it is common to find that in the headers.
    # It is not required, however, so it could be missing even if the headers were
    # parsed correctly.

    # Attempt 1: Not explicitly reading the headers
    content_type = response.getheader("content-type") 
    content_length = response.getheader("content-length")
    transfer_encoding = response.getheader("transfer-encoding") 

    # Attempt 2: Maybe we needed to explicitly read the headers
    if not content_type:
        response.readheaders()
        content_type = response.getheader("content-type") 
        content_length = response.getheader("content-length")
        transfer_encoding = response.getheader("transfer-encoding") 
        # Attempt 3: Parse the fields from the header manually
        if not content_type:
            header_lower = buffered_header.lower()
           
            if "content-length" in header_lower:
                content_length_begin = header_lower.find("content-length")
                content_length_end = header_lower[content_length_begin:].find("\n") + content_length_begin
                content_length_header = header_lower[content_length_begin:content_length_end + 1]
                if VERBOSE: logging.debug("Manually parsed out content length header: %s" % content_length_header)
                if content_length_header:
                    content_length = content_length_header.split(":")[1].strip()
                    if VERBOSE: logging.debug("Parsed out this content length: %s" % content_length)
                else:
                    content_length = None

            if "transfer-encoding" in header_lower:
                transfer_encoding_begin = header_lower.find("transfer-encoding")
                transfer_encoding_end = header_lower[transfer_encoding_begin:].find("\n") + transfer_encoding_begin
                transfer_encoding_header = header_lower[transfer_encoding_begin:transfer_encoding_end + 1]
                if VERBOSE: logging.debug("Manually parsed out transfer_encoding header: %s" % transfer_encoding_header)
                if transfer_encoding_header:
                    transfer_encoding = transfer_encoding_header.split(":")[1].strip()
                    if VERBOSE: logging.debug("Parsed out this transfer_encoding: %s" % transfer_encoding)
                else:
                    transfer_encoding = None
    return (content_length, transfer_encoding)

def setup_next_state_from_header(client_conn):
    buffered_header = HTTP_client_req_info[client_conn]["buffered_header"]
    method = HTTP_client_req_info[client_conn]["method"]

    if VERBOSE: logging.debug("[%s] Buffered header: |%s|" % (str(client_conn), buffered_header))
    
    (content_length, transfer_encoding) = get_info_from_header(buffered_header)

    reset_client_req_info(client_conn)

    if transfer_encoding and transfer_encoding.lower() == "chunked":
        HTTP_client_req_info[client_conn]["state"] = "READING_CHUNK_SIZE"
        return "READING_CHUNK_SIZE"
    elif content_length:
        # We have a body, but it's empty - don't bother reading it
        if int(content_length) == 0:
            reset_client_req_info(client_conn)
            return "IDLE"

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


        status_code = int(status.split(None)[0])
        if VERBOSE: logging.debug("Status code: %s, whole status: %s. Status code == 304? %s" % (
            status_code, status, status_code == 304))

        if (status_code == 204 or status_code == 304 or
            100 <= status_code < 200 or      # 1xx codes
            method == "HEAD"):
            if VERBOSE: logging.debug("[%s] Received a response that does not have a body, so going back to idle"  % str(client_conn))
            reset_client_req_info(client_conn)
            return "IDLE"
        if VERBOSE: logging.debug("ERROR: Status line was fine, and code was not one where it should have no body. Status line: %s. Returning IDLE" % status_line)
        return "IDLE"
    
def forward_chunk_to_client(client_conn, server_conn, data):
    content_length = int(HTTP_client_req_info[client_conn]["curr_content_length"])
    bytes_sent = int(HTTP_client_req_info[client_conn]["bytes_sent"])

    # Each chunk is terminated by a two-byte \r\n. We manually add on these two
    # bytes here to determine how many bytes to actually send
    bytes_to_send = min(content_length + 2 - bytes_sent, len(data))

    if bytes_to_send > 0:
        if MODIFY_CONTENT:
            data = modify_content(data)
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
    if MODIFY_CONTENT:
        data = modify_content(data)
    forward_bytes(server_conn, data[:bytes_to_send])

    bytes_sent += bytes_to_send
    HTTP_client_req_info[client_conn]["bytes_sent"] = bytes_sent

    if bytes_sent == content_length:
        reset_client_req_info(client_conn)
        if VERBOSE: logging.debug("[%s] Bytes_sent == content_length, state is now %s" % (
            str(client_conn), HTTP_client_req_info[client_conn]["state"]))
    
    return data[bytes_to_send:]

def remove_accept_encoding(data):
    request = HTTPRequest(data)
    try:
        if request.headers["accept-encoding"]:
            index = data.find("Accept-Encoding")
            terminating_newline = data[index:].find("\n") + index
            return data[:index] + data[terminating_newline + 1:]
    except KeyError:
        pass
    except AttributeError:
        if VERBOSE: logging.debug("Hmm caught another exception. Here's the data\n%s" % data)
    return data

def modify_content(data):
    # Replaces all instances of CONTENT_REPLACE_TARGET (case insensitive) with "Fahad"
    pattern = re.compile(re.escape(CONTENT_REPLACE_TARGET), re.IGNORECASE)
    data = pattern.sub("Fahad", data)
    return data

def reset_client_req_info(client_conn):
    if VERBOSE: logging.debug("Resetting client info for %s" % str(client_conn))
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
        if VERBOSE: logging.debug("ERROR: Server (%s) for client %s sent data while in idle state. Data read: %s" %
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
        if VERBOSE: logging.debug("ERROR: Unknown state %s" % state)

    if unread_data:
        if VERBOSE: logging.debug("[%s] Some data left unread after handling. Calling handle again" % str(client_conn))
        handle_HTTP_server_response(client_conn, server_conn, unread_data)

###############################################################################
#                          RATE LIMITING UTILS                                #
###############################################################################

def get_max_bandwidth(client_conn, bytes_wanted):
    # Returns the max bandwidth available for the given client, defined as the
    # number of tokens in the bucket, or the number of bytes requested by the
    # client (whichever is smaller)

    client_ip = client_IPS[client_conn]

    global limiter
    # Tokens are only replenished when they are consumed, so for us to get an
    # accurate reading on how many tokens are available, we must consume. The
    # smallest amount we can consume is 1 token
    limiter.consume(client_ip, 1)

    # Add back what we consumed above
    curr_available = limiter._storage.get_token_count(client_ip) + 1
    if VERBOSE: logging.debug("Getting min of (%f and %f - curr avail)" % (bytes_wanted, curr_available))

    return min(bytes_wanted, curr_available)

def set_token_quantity(client_conn, num_tokens):
    # Sets the number of tokens of a given client to the number of tokens
    # passed in

    # This function works by manipulating how replenish is implemented, since
    # there is no explicit way to set the number of tokens per bucket.
    # replenish() sets the number of tokens to:
    #   min(capacity, tokens_in_bucket + (rate * (now - last_replenished_at)))
    # where capacity and rate are passed in as parameters. We aim to force
    # replenish to always use the value for capacity that we pass in.

    client_ip = client_IPS[client_conn]

    if VERBOSE: logging.debug("Resetting token count for %s to %f" % (str(client_conn), num_tokens))
    # Replenish with rate equivalent to infinity, which should force the capacity
    # to be used. There's still a chance this could end up being lower if replenish
    # was called recently (unless num_tokens is 0).
    limiter._storage.replenish(client_ip, float('inf'), num_tokens)
   
    if limiter._storage.get_token_count(client_ip) != num_tokens:
        if VERBOSE: logging.debug("ERROR: Token count should be %f but it's actually %f" %
                (num_tokens, limiter._storage.get_token_count(client_ip)))


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

def open_HTTP_server_connection(host):
    # setup a new HTTP connection
    server_conn = socket(AF_INET, SOCK_STREAM)
    hostname, port = split_host(host)
    if port is None:
        port = 80
    server_conn.connect((hostname, port))
    return server_conn


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
