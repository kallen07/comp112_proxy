from socket import *
import sys
from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO
import httplib
import traceback
from threading import Thread
import Queue
import token_bucket
from requests.models import Response

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
RATE = 100
CAPACITY = 500
storage = token_bucket.MemoryStorage()
limiter = token_bucket.Limiter(RATE, CAPACITY, storage)

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
        #print "on thread", id, "client:", request[1]
        # Key is client address
        handle_client_request(*request)
        


################### CLIENT #######################
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
    print formatted_res
    connection.sendall(formatted_res)
    #connection.sendall(res)


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
                # Key is client address
                success = limiter.consume(client_address)
                print "CONSUMED TOKEN" if success else "COULD NOT CONSUME TOKEN"
                if not success:
                    send_rate_limiting_error(connection, client_address, request)
                    # TODO: close connection ehre
                    break
                response = get_response_from_server(request)
                send_response_to_client(response, connection)
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



        
def send_response_to_client(response, connection):
    headers = response.getheaders()
    http_version = '1.0' if response.version is 10 else '1.1'

    formatted_header = 'HTTP/{} {} {}\r\n{}\r\n\r\n'.format(
        http_version, str(response.status), response.reason,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in headers)
    )
    print >> sys.stderr, "Sending headers"
    connection.sendall(formatted_header)
    print(formatted_header)

    # Chunked responses are forwarded using the approach found here:
    # https://stackoverflow.com/questions/24500752/how-can-i-read-exactly-one-response-chunk-with-pythons-http-client
    if response.getheader('transfer-encoding', '').lower() == 'chunked':
        print "Sending data as chunked"

        def send_chunk_size():
            # size_str will contain the size of the current chunk in hex
            size_str = response.read(2)
            while size_str[-2:] != b"\r\n":
                size_str += response.read(1)

            print >> sys.stderr, "chunk size:", size_str[:-2], "(", int(size_str[:-2], 16), ")"

            # adds hex string plus \r\n delimeter to body
            connection.sendall(size_str)
            return int(size_str[:-2], 16)

        def send_chunk_data(chunk_size):
            # data for chunk + \r\n at the end
            data = response.read(chunk_size + 2)

            # TODO: Error case if data[-2:] != b"\r\n". Maybe throw a custom exception
            # that the parent thread can catch? Or just silently give up and rely on
            # eventual retries? Or maybe just manually add the delimeter? Not sure what
            # the best approach is.
            if data[-2:] != b"\r\n":
                print >> sys.stderr, "ERROR: Chunk did not end in newline-carriage return"
            connection.sendall(data)

        while True:
            chunk_size = send_chunk_size()

            if (chunk_size == 0):
                print >> sys.stderr, "Sending terminating chunk"
                connection.sendall(b"\r\n")
                break
            else:
                print >> sys.stderr, "Sending chunk of size", chunk_size
                send_chunk_data(chunk_size)

    else:
        print >> sys.stderr, "Reading response in one go (not chunked)"
        connection.sendall(response.read())



################### SERVER #######################
def get_response_from_server(request):
    hostname = request.headers["host"]

    conn = httplib.HTTPConnection(hostname)
    print >> sys.stderr, "Got connection to server"

    # TODO: Chrome does not allow cookies to be set by localhost. Logins will not work with chrome.
    # TODO: Cookies seem to be acting up in Firefox too. I get logged out after a couple of page
    # changes
    if request.body:
        conn.request(request.command, request.path.split(hostname)[1], body=request.body, headers=dict(request.headers))
    else:
        conn.request(request.command, request.path.split(hostname)[1], headers=dict(request.headers))

    res = conn.getresponse()
    # TODO: Flash(?) games don't seem to load properly. (Go to neopets->Game Room, and click on any game.)

    # Response is marked as not being chunked to allow us to send the chunks
    # correctly later on
    res.chunked = False

    # TODO close this connection or reuse it
    # conn.close()
    return res


if __name__ == "__main__":
    main()
