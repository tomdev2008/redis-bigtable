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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import redis.clients.jedis.Jedis;

public class RedisHashRecordReader extends RecordReader<Text,Text>
{
	private Iterator<Entry<String, String>> keyValueMapIter = null;
	private Text key = new Text();
	private Text value = new Text();
	private float processedKVs = 0;
	private float totalKVs = 0;
	private Entry<String, String> currentEntry = null;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		String host = split.getLocations()[0];
		String hashKey = ((RedisHashInputSplit) split).getHashKey();
		Jedis jedis = new Jedis(host);
		jedis.connect();
		jedis.getClient().setTimeoutInfinite();
		totalKVs = jedis.hlen(hashKey);
		keyValueMapIter = jedis.hgetAll(hashKey).entrySet().iterator();
		System.out.println("Got " + totalKVs + " from " + hashKey);
		jedis.disconnect();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (keyValueMapIter.hasNext())
		{
			currentEntry = keyValueMapIter.next();
			key.set(currentEntry.getKey());
			value.set(currentEntry.getValue());
			return true;
		}
		else
		{
			return false;
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException
	{
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		return processedKVs / totalKVs;
	}

	@Override
	public void close() throws IOException
	{
	}

}