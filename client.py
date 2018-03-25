from socket import *
import sys

from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO

import httplib

class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = StringIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message

def main():

    MAX_HEADER_BYTES = 8000

    serverSocket = socket(AF_INET, SOCK_STREAM)
    server_address = ('localhost', 8080)
    print >>sys.stderr, 'starting up on %s port %s' % server_address
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind(server_address)
    serverSocket.listen(1)
    serverSocket.settimeout(60)
    while True:
        print >>sys.stderr, 'waiting for a connection'
        connection, client_address = serverSocket.accept()

        try:
            print >> sys.stderr, 'connection from', client_address
            data = connection.recv(MAX_HEADER_BYTES)
            request = HTTPRequest(data)
            print >> sys.stderr, 'received "%s"' % data
            if data:
                #print >>sys.stderr, 'received data: %s' % data
                response = get_response_from_server(request)
                send_reponse_to_client(response, connection)
                # connection.sendall(ABOUT_ME)
            else:
                print >> sys.stderr, 'no more data from', client_address
                break
        finally:
            # Clean up the connection
            connection.close()



################### CLIENT #######################
def send_reponse_to_client(response, connection):
    headers = response.getheaders()
    #headers.append(("Content-Length", ""))

    #'1.0' if response.version is 10 else '1.1'
    formatted_res = 'HTTP/{} {} {}\r\n{}\r\n\r\n{}'.format(
        '1.0', str(response.status), response.reason,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in headers),
        response.read()
    )

    print('HTTP/{} {} {}\r\n{}\n'.format(
        '1.0', str(response.status), response.reason,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in headers)
    ))
    #print formatted_res
    connection.sendall(formatted_res)


################### SERVER #######################
def get_response_from_server(request):
    print >> sys.stderr, "attempting to connect to server"
    hostname = request.headers["host"]
    conn = httplib.HTTPConnection(hostname)
    conn.request("GET",request.path.split(hostname)[1])
    res = conn.getresponse()
    return res

if __name__ == "__main__":
    main()
