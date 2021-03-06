# Mailgun - Catchall Tooolkit

Catchall is a set of services that can be deployed to save and serve statistics based on email events
You can contact me at henriot.rem@gmail.com for any questions

## Features

- It uses MongoDB as the main database
- It uses Uhaha framework to create a fault tolerant in-memory database for the workers
- Statistics are refreshed in mongo every 30sec

... a lot of things could be improved and changed based on additional analysis

## Building

Build all the binaries in %GOPATH%/bin

```
go install ./...
cd %GOPATH%/bin
```

## Getting started

```
./simulation

curl --location --request PUT 'localhost:22001/events/test.com/delivered'
curl --location --request PUT 'localhost:22001/events/test.com/bounced'
```

Check http://localhost:8085/domains/test.com to get the status of the domain

## Running

Let's create a first cluster of 3 nodes

```
./worker -d cluster-1 -n 1 -a :11001
./worker -d cluster-1 -n 2 -a :11011 -j 127.0.0.1:11001
./worker -d cluster-1 -n 3 -a :11021 -j 127.0.0.1:11001
./worker-api 22001 127.0.0.1:11001
```

Now we have a fault-tolerant three node cluster up and running.

```
./worker -d cluster-2 -n 1 -a :11002
./worker -d cluster-2 -n 2 -a :11012 -j 127.0.0.1:11002
./worker -d cluster-2 -n 3 -a :11022 -j 127.0.0.1:11002
./worker-api 22002 127.0.0.1:11002
```

...second one

```
./worker -d cluster-3 -n 1 -a :11003
./worker -d cluster-3 -n 2 -a :11013 -j 127.0.0.1:11003
./worker -d cluster-3 -n 3 -a :11023 -j 127.0.0.1:11003
./worker-api 22003 127.0.0.1:11003
```

...third one

```
./master 127.0.0.1:27017 127.0.0.1:11001,127.0.0.1:11011,127.0.0.1:11021 127.0.0.1:11002,127.0.0.1:11012,127.0.0.1:11022 127.0.0.1:11003,127.0.0.1:11013,127.0.0.1:11023
```

Master aggregator service started

```
./master-api 127.0.0.1:27017 8085
```

Master web server to get the status of the domains : GET http://127.0.0.1:8085/domains/%domain%

## Simulation

The manual start can be performed just by running the simulation binary with the optional parameter

```
./simulation [nCluster] [nNodes]
```

## Benchmark

Run a single thread firing synchronus calls on the 3 web servers 2200[1-3]

```
./benchmark 200000 40 http://127.0.0.1:22001,http://127.0.0.1:22002,http://127.0.0.1:22003
```
