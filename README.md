![cxo logo](https://user-images.githubusercontent.com/26845312/32426759-2a7c367c-c282-11e7-87bc-9f0a936046af.png)


[中文文档](./README-CN.md)


CX Object System
================

[![Build Status](https://travis-ci.org/skycoin/cxo.svg)](https://travis-ci.org/skycoin/cxo)
[![GoReportCard](https://goreportcard.com/badge/skycoin/cxo)](https://goreportcard.com/report/skycoin/cxo)
[![Telegram group link](telegram-group.svg)](https://t.me/joinchat/B_ax-A6oCR9eQuAPiJtvaw)

The CXO is objects system, goal of which is sharing any objects. The CXO
is low level and designed to build application on top of it

### Get Started and API Documentation

See [CXO wiki](https://github.com/skycoin/cxo/wiki/Get-Started) to get this information

### API Documentation

See [CXO wiki](https://github.com/skycoin/cxo/wiki) to get this information

### Installation

To get the release use
```
go get -u -t github.com/skycoin/cxo/...
```
Test all packages
```
go test -cover -race github.com/skycoin/cxo/...
```

### Docker

```
docker run -ti --rm -p 8870:8870 -p 8871:8871 skycoin/cxo
```


### Development

- [telegram group](https://t.me/joinchat/B_ax-A6oCR9eQuAPiJtvaw)

#### Packages

- `cmd` - apps
  - `cxocli` - CLI is admin RPC based tool to control any CXO-node
    ([wiki/CLI](https://github.com/skycoin/cxo/wiki/CLI)).
  - `cxod` - an average CXO daemon that accepts all subscriptions
- `cxoutils` - basic utilities (database cleaning)
- `data` - database interfaces, implemntations, objects and errors
  - `data/cxds` - CX data store is implementation of key-value store
  - `data/idxdb` - implementation of index DB
  - `data/tests` - tests for the `data` interfaces
- `node` - TCP transport for CXO
  - `node/log` - logger
  - `node/msg` - internal node-to-node protocol
- `skyobject` - CXO core: encode/decode, etc
  - `registry` - schemas, types, etc

And

- [`intro`](./intro) - examples


#### Formatting and Coding Style

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

#### Version

Dependencies are managed with [dep](https://golang.github.io/dep). The master
branch of the repository points to latest stable release. Actually, it points
to beta-release for now.

##### Versioning

The CXO uses MAJOR.MINOR versions. Where MAJOR is

- API cahgnes
- protocol chagnes
- data representation chagnes (database format)

and MINOR is
- small API changes
- fixes
- improvements

Thus, DB files are not compatible between different major versions. Nodes
with different major versions can't communicate. Saved data may have another
representation.

###### Versions

- 2.1 d4e4ab573c438a965588a651ee1b76b8acbb3724
- 3.0 f87f9c197597bc78ec301d32b4678f50e9f52726
- 4.0 master (beta)

---
