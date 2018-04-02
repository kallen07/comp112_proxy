from socket import *
import sys
from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO
import httplib
import traceback
from threading import Thread
import Queue
import token_bucket
from collections import defaultdict

MAX_HEADER_BYTES = 8000
MAX_NUM_THREADS = 10
requests = Queue.Queue()
# From documentation: 
# rate: Number of tokens per second to add to the
#   bucket. Over time, the number of tokens that can be
#   consumed is limited by this rate. Each token represents
#   some percentage of a finite resource that may be
#   utilized by a consumer.
# capacity: Maximum number of tokens that the bucket
#   can hold. Once the bucket is full, additional tokens
#   are discarded.

# Rate and capacity units: number of requests (per client)
rate = 100
capacity = 500
storage = token_bucket.MemoryStorage()
limiter = token_bucket.Limiter(rate, capacity, storage)

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


################### MAIN #######################
def main():
    port = int(sys.argv[1])

    # create master socket
    serverSocket = socket(AF_INET, SOCK_STREAM)
    server_address = ('localhost', port)
    print >> sys.stderr, 'starting up on %s port %s' % server_address
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind(server_address)
    serverSocket.listen(1)

    # create thread pool
    threads = []
    for i in xrange(MAX_NUM_THREADS):
        t = Thread(target = consumer_thread, args = [i])
        threads.append(t)
        t.start()

    # accept new socket connections
    while True:
        print >> sys.stderr, '=================== waiting for a connection =================='
        connection, client_address = serverSocket.accept()
        print >> sys.stderr, '=================== new connection from', client_address, '==================='
        connection.settimeout(5)
        requests.put((connection, client_address))
    

#################### THREADING ###################
def consumer_thread(id):
    while True:
        request = requests.get()
        print "on thread", id, "client:", request[1]
        # Key is client address
        limiter.consume(request[1])
        handle_client_request(*request)
        


################### CLIENT #######################
def handle_client_request(connection, client_address):
    # accept new requests from a connection
    # TODO determine if we need this while True loop
    while True:
        try:
            print >> sys.stderr, '=================== reading from', client_address, '==================='
            data = connection.recv(MAX_HEADER_BYTES)
            request = HTTPRequest(data)
            print >> sys.stderr, 'received "%s"' % data
            if data:
                response = get_response_from_server(request)
                send_reponse_to_client(response, connection)
            else:
                print >> sys.stderr, 'no more data from', client_address
                break
        except:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Unexpected error:", sys.exc_info())
            print traceback.print_tb(sys.exc_info()[2])
            # if sys.exc_info
            print >> sys.stderr, '=================== CLOSING SOCKET =================='
            connection.close()
            break

        
def send_reponse_to_client(response, connection):
    headers = response.getheaders()
    #headers.append(("Content-Length", ""))

    #'1.0' if response.version is 10 else '1.1'
    # TODO update HTML version so it isn't hardcoded as 1.0
    formatted_res = 'HTTP/{} {} {}\r\n{}\r\n\r\n{}'.format(
        '1.0', str(response.status), response.reason,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in headers),
        response.read()
    )

    print('HTTP/{} {} {}\r\n{}\n'.format(
        '1.0', str(response.status), response.reason,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in headers)
    ))
    connection.sendall(formatted_res)



################### SERVER #######################
def get_response_from_server(request):
    hostname = request.headers["host"]
    # print "***************************************"
    # print request.command
    # if request.body:
    #     print request.body
    # print "***************************************"

    conn = httplib.HTTPConnection(hostname)

    # NEEDSWORK: Chrome does not allow cookies to be set by localhost. Logins will not work
    if request.body:
        conn.request(request.command, request.path.split(hostname)[1], body=request.body, headers=dict(request.headers))
    else:
        conn.request(request.command, request.path.split(hostname)[1], headers=dict(request.headers))
    res = conn.getresponse()

    # TODO close this connection or reuse it
    return res


if __name__ == "__main__":
    main()
