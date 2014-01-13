redis-bigtable
==============
## Brief Introduction

Redis additions that mimic Accumulo's structure with Hadoop connectors..  If you're familiar with BigTable NoSQL databases, like Accumulo or HBase, this is a redis implementation of (a simplified) Accumulo. It does not have customized iterators or use distributed locking through Apache Zookeeper (yet). 

This project was inspired because I really enjoy the constraint-based, key-value paradigm Accumulo offers, but I dislike how much heapspace it hogs. So, you won't find tablet servers, garbage collectors or monitors.

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

(EDIT: I'm working on getting the code stable enough to submit to Maven Central - 1/13/14)

I don't have a private nexus instance to host the jar/pom file. So at the moment, you'll have to build the uber-jar.

From the root directory, run `mvn clean install`. It is HIGHLY recommened you do not use the `-DskipTests` flag. Tests located in `src/test/java` validate your connection to redis.

## Quick Start

### The RedisForeman

The RedisForeman connects to redis (via Jedis) and provides the interface to make table operations.

* Create/Delete a table
    * `redis-bigtable` is liberal about table names. The only restriction is that table names cannot start with a `!`. Those are reserved for system tables. 


            byte[] exampleTable = "example".getBytes();
            RedisForeman foreman = new RedisForeman();
            //Create the table "example"
            foreman.createTable(exampleTable); 
            //Remove the table "example"
            foreman.deleteTable(exampleTable);

* Remove all tables


        RedisForeman foreman = new RedisForeman();
        foreman.deleteTables();
        
* Table existance


        RedisForeman foreman = new RedisForeman();
        foreman.tableExists("example".getBytes());
        
* Writing RedisBigTableKeys
    * `redis-bigtable` is organized into key and value pairs, muuch like a HashMap. The key is composed of 3 parts: a row, a column family, and a column qualifier. You can regard the qualifier as the key's name, and the row and family as a namespace for the key.
    * Row, column family and column qualifier parts allow for any name other than a single `*` (which is reserved for wild card retrievals) and the ASCII record separator (0x1E).


            byte[] table = "People".getBytes();
            byte[] row = "Cam Cook".getBytes();
            byte[] cf = "NAME_PART".getBytes();
            byte[] cq = "FIRST_NAME".getBytes();
            RedisBigTableKey key = new RedisBigTableKey(row,cf,cq);
            byte[] value = "Cam".getBytes();
            RedisForeman foreman = new RedisForeman();
            
            //writes the entry ((Cam Cook, NAME_PART, FIRST_NAME), (Cam)) to table People
            foreman.write(table,key,value);

     

* Retrieving Key/Vaue pairs
    * You can retrieve key/value pairs by row, column family or column qualifier. Each of these parts can accept a wild card character (`*`)


            RedisForeman foreman = new RedisForeman();
            byte[] exampleTable1 = "example".getBytes();
            //create "exammple" table
            foreman.createTable(exampleTable1);
            
            byte[] row = "row".getBytes();
            byte[] row2 = "row2".getBytes();
            byte[] a = "a".getBytes();
            byte[] b = "b".getBytes();
            byte[] c = "c".getBytes();
            byte[] d = "d".getBytes();
            
            // writes (row, a,b) (empty) to table "example"
            foreman.write(exampleTable1, row, a, b, Utils.EMPTY);
            // writes (row2,b,c) (empty) to table "example"
            foreman.write(exampleTable1, row2, b, c, Utils.EMPTY);
            
            byte[] wildCard = new byte[] { Utils.WILD_CARD };
            //Retrieves all rows in table "example"
            Map<RedisBigTableKey, byte[]> getByRowEntries = foreman.getByRow(exampleTable1, wildCard);

### Hadoop Connectors

* [RedisBigTableInputFormat](https://github.com/Ccook/redis-bigtable/blob/master/src/main/java/edu/american/student/redis/hadoop/RedisBigTableInputFormat.java)
* [RedisBigTableInputSplit](https://github.com/Ccook/redis-bigtable/blob/master/src/main/java/edu/american/student/redis/hadoop/RedisBigTableInputSplit.java)
* [RedisBigTableKey](https://github.com/Ccook/redis-bigtable/blob/master/src/main/java/edu/american/student/redis/hadoop/RedisBigTableKey.java)
* [RedisBigTableOutputFormat](https://github.com/Ccook/redis-bigtable/blob/master/src/main/java/edu/american/student/redis/hadoop/RedisBigTableOutputFormat.java)
* [RedisBigTableRecordReader](https://github.com/Ccook/redis-bigtable/blob/master/src/main/java/edu/american/student/redis/hadoop/RedisBigTableRecordReader.java)
* [RedisBigTableRecordWriter](https://github.com/Ccook/redis-bigtable/blob/master/src/main/java/edu/american/student/redis/hadoop/RedisBigTableRecordWriter.java)

## Examples

## License

* Apache License version 2
