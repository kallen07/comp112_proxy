from socket import *
import sys

def main():

    serverSocket = socket(AF_INET, SOCK_STREAM)
    server_address = ('localhost', 8080)
    print >>sys.stderr, 'starting up on %s port %s' % server_address
    serverSocket.bind(server_address)
    serverSocket.listen(1)
    while True:
        print >>sys.stderr, 'waiting for a connection'
        connection, client_address = serverSocket.accept()

        try:
            print >>sys.stderr, 'connection from', client_address

            # Receive the data in small chunks and retransmit it
            while True:
                data = connection.recv(256)
                print >>sys.stderr, 'received "%s"' % data
                if data:
                    #print >>sys.stderr, 'received data: %s' % data
                    connection.sendall(data)
                else:
                    print >>sys.stderr, 'no more data from', client_address
                    break
                
        finally:
            # Clean up the connection
            connection.close()

if __name__ == "__main__":
    main()