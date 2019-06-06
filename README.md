# oram-proxy
Proxy that allows cloud applications to use Oblivious RAM (ORAM). The proxy mimics a cloud object storage service (e.g OpenStack Swift) and runs the requests it receives through an ORAM algorithm. The ORAM algorithm can either use the local file system or a cloud storage service as its backend. This code is based on the [CURIOUS framework](http://seclab.soic.indiana.edu/curious/) by Bindschaedler et al.

## Compiling

This project uses the gradle build system. To compile the code, run the following command
````sh
gradle build
````

## Usage

To select which ORAM scheme to use and other parameters, edit the file `build.gradle`. If credentials need to be provided (e.g when using Amazon S3 as a backend) they should be placed in a file called `credentials.file` in the root directory of the project. After configuring the proxy, run the following command to start it:

````sh
gradle run
````

By default, the proxy exposes a Swift API over HTTP at port 8080.