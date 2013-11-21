/**
 * <br>
 * Licensed under the Apache License, Version 2.0 (the "License");<br>
 * you may not use this file except in compliance with the License.<br>
 * You may obtain a copy of the License at<br>
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0<br>
 * <br>
 * Unless required by applicable law or agreed to in writing, software<br>
 * distributed under the License is distributed on an "AS IS" BASIS,<br>
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.<br>
 * See the License for the specific language governing permissions and<br>
 * limitations under the License.<br>
 */
package edu.american.student.redis.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import redis.clients.jedis.Jedis;

/**
 * This class is template to write only Text keys and Text values 
 * @author cam
 *
 */
public class RedisHashRecordWriter extends RecordWriter<Text, Text>
{
	private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
	private String hashKey = null;

	public RedisHashRecordWriter(String hashKey, String hosts)
	{
		this.hashKey = hashKey;
		int i = 0;
		for (String host : hosts.split(","))
		{
			Jedis jedis = new Jedis(host);
			jedis.connect();
			jedisMap.put(i++, jedis);
		}
	}

	@Override
	/**
	 * The write method is what will actually write the key value pairs out to Redis
	 */
	public void write(Text key, Text value) throws IOException, InterruptedException
	{
		/* Get the Jedis instance that this key/value pair will be written to. */
		Jedis j = jedisMap.get(Math.abs(key.hashCode()) % jedisMap.size());
		/* Write the key/value pair*/
		j.hset(hashKey, key.toString(), value.toString());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException
	{
		/* For each jedis instance, disconnect it*/
		for (Jedis jedis : jedisMap.values())
		{
			jedis.disconnect();
		}
	}

}