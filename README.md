# comp112_proxy


HTTPS bidict -> mapping between server and client sockets

potential features to add:
- blacklists for sites
- hosting on a server so other people can try it out
- maybe one more?

Can integrate data read for HTTP and HTTPS so that we have the same code path for ratelimiting (consume the max number of tokens, put back what you didn't use - ie if the read wasn't big enough, then process that data, which will be different for http vs https)

