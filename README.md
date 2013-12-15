redis-bigtable
==============

BigTable inspired redis additions with connectors for Hadoop 1.0.3. 

## Installation

To avoid configuration files, there are certain environment variables to set for the RedisForeman to properly connect to your redis instance.

### Linux

Put these values in `/etc/environment` & restart

    REDIS_HOST=<IP address of your redis instance e.g 127.0.0.1>
    REDIS_PORT=<PORT of your redis instance, e.g 6379>

### Mac

Put these values in `/etc/profile` & restart

    REDIS_HOST=<IP address of your redis instance e.g 127.0.0.1>
    REDIS_PORT=<PORT of your redis instance, e.g 6379>
    

### Building the uber-jar

I don't have a private nexus instance to host the jar/pom file. So at the moment, you'll have to build the uber-jar.

From the root directory, run `mvn clean install`. It is HIGHLY recommened you do not use the `-DskipTests` flag. Tests located in `src/test/java` validate your connection to redis.

## Quick Start

### The RedisForeman

### Hadoop Connectors

## Examples

## License

* Apache License version 2
