split
=====

Since the CXO has max object size limit, we can't use very large objects
in the CXO. By default, the limit is 16MB and the limit required to protect
against DoS attacks. The limit is configurable and should be equal for all
nodes of swarm they related.

There are `Split` and `Concat` methods of `skyobject.Container` that are
helpers to turn an `io.Reader` to Merkle-tree and turn the Merkle-tree
to an `io.Writer`.

This is a way to save big objects in the CXO.


For the example, the MaxObjectSiz reduced to 1024 - the lowest possible
bound of the limit. Actually, data saved with prefix (4 bytes) that describes
length of the data.

The example loads files from fielsystem and stores them in CXO. And after,
if loads the files from CXO and stores in fielsystem. Result is

```
$ md5sum *.png
5bbb0a47dbb39a1406eb7a22003aa281  skycoin-black-from-cxo.png
5bbb0a47dbb39a1406eb7a22003aa281  skycoin-black.png
c2f00b51e21429159336607156f44d55  skycoin-white-from-cxo.png
c2f00b51e21429159336607156f44d55  skycoin-white.png
```
