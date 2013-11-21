redis-bigtable
==============

BigTable inspired connector for Hadoop 1.0.3. 

### What you get out of the box

* The RedisForeman to intereact with redis. 
  * It's based off Jedis
  * It enforces BigTable constraints on redis
    * Every BigTable Entry has the following
    * &lt;table&gt; &lt;row&gt; &lt;column family&gt; &lt;column qualifier&gt; &lt;value&gt;
    * For Example:
  
          `ROW----CF------CQ-------VALUE`

          `PLANTS-COLEUS--LENGTH---5 `
          
         ` PLANTS-COLEUS--COLOR---GREEN`

* The Map/Reduce InputFormat and OutputFormat to build M/R jobs reading/writing to Redis


### Work In Progress

This project is  a work in process. Assume Unstable. I hope to finish it by the end of the wwek Nov 24 2013. 
I'll be sure to document everything.
