import logging
import argparse
from timeit import default_timer as timer


import requests
import sys
import traceback
import httplib


def retrieve_large_file(filename, proxy_port):
    # proxies = {"localhost:8080"}
    # s = requests.Session()
    # s.proxies = proxies
    # print >> sys.stderr, "Set up session"

    host = "localhost:8000"
    headers= {"Host" : host}

    conn = httplib.HTTPConnection("localhost", proxy_port)
    # print >> sys.stderr, "Connection opened"
    conn.request('GET', "%s/%s" % (host, filename), headers=headers)
    # print >> sys.stderr, "Request sent"
    res = conn.getresponse()
    print  >> sys.stderr, "\tResponse: %d with reason %s" % (res.status, res.reason)
    # print >> sys.stderr, "Got response"
    # print >> sys.stderr, dir(res)
    # print >> sys.stderr, res.getheaders()
    # if res.getheader("transfer-encoding", "").lower() == "chunked":
    #   print >> sys.stderr, "Response says it's chunked"
    # else:
    #   print >> sys.stderr, "Response says it's not chunked. Transfer encoding: %s" % res.getheader("transfer-encoding", "")
    res.read()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", help="proxy's port", type=int, default=8080, required=False)
    parser.add_argument("--num_samples", help="number of samples to run", type=int, default=5, required=False)
    parser.add_argument("--files", nargs="*", help="files to test downloading", required=False)
    args = parser.parse_args()
    
    samples = {}
    if not args.files:
        files = ["logs_apr9_1217am", "logs_apr10_637pm_connection_close", "logs_apr11_529", "halfGB.txt", "1000000B.dat", "500MB.dat"]
    else:
        files = args.files

    for file in files:
        print  >> sys.stderr, "----------------------------------------------"
        print  >> sys.stderr, "Testing file %s..." % file
        for i in xrange(0, args.num_samples):
            if file not in samples:
                samples[file] = []
            start = timer()
            try:
                retrieve_large_file(file, args.port)
            except:
                print  >> sys.stderr, "Test for %s failed with an exception" % file
                print  >> sys.stderr, str(sys.exc_info())
                print >> sys.stderr, (traceback.print_tb(sys.exc_info()[2]))
                continue
            end = timer()
            print >> sys.stderr, "\tSample %d: %f" % (i, end - start)
            samples[file].append(end-start)
        if len(samples[file]) > 0:
            print >> sys.stderr, "Average: \n\t%f" % (sum(samples[file])/float(len(samples[file])))
        else:
            print  >> sys.stderr, "No average. All samples threw an exception."



if __name__ == "__main__":
    main()
