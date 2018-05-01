# Comp 112 Proxy


# Mechanics

Please run the following commands to install packages

``pip install bidict``

``pip install token-bucket``

# Running our proxy:

**Without any special features**

``python proxy.py [port #]``

**With very verbose logging**

``python proxy.py -v [port #]``

**Bind to a specific IP**

``python proxy.py -a [IP address] [port #]``

**Enable rate limiting**

``python proxy.py --bandwidth=[value in bytes per second] --burst_rate=[value in bytes per second] [port #]``

**Blacklist sites**

``python proxy.py -b [filename] [port #]``

Note: file should contain a list of newline separated hostnames

**Content substitution**

``python proxy.py --fahad_keyword=[5 letter word] [port #]``
