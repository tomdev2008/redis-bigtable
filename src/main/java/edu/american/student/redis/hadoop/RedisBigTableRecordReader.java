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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.american.student.redis.MessageFactory;
import edu.american.student.redis.foreman.RedisForeman;
import edu.american.student.redis.foreman.RedisForemanException;

public class RedisBigTableRecordReader extends RecordReader<RedisBigTableKey, Text>
{
	private Iterator<Entry<RedisBigTableKey, byte[]>> keyValueMapIter = null;
	private Entry<RedisBigTableKey, byte[]> currentEntry = null;
	private RedisBigTableKey key = null;
	private Text value = new Text();
	private float totalKVs = 0;
	private float processedKVs = 0;

	@Override
	public void close() throws IOException
	{

	}

	@Override
	public RedisBigTableKey getCurrentKey() throws IOException, InterruptedException
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
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		String[] locations = split.getLocations();
		RedisBigTableKey key = RedisBigTableKey.inflate(locations[0].getBytes());
		byte[] row = key.getRow();
		byte[] cf = key.getColumnFamily();
		byte[] cq = key.getColumnQualifier();
		RedisForeman foreman = new RedisForeman();
		foreman.connect();
		Map<RedisBigTableKey, byte[]> keyValues;
		try
		{
			keyValues = foreman.getByQualifier(locations[1].getBytes(), row, cf, cq);
			totalKVs = keyValues.size();
			keyValueMapIter = keyValues.entrySet().iterator();
		}
		catch (RedisForemanException e)
		{
			throw new IOException(MessageFactory.objective("Read Split").toString(), e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (keyValueMapIter.hasNext())
		{
			currentEntry = keyValueMapIter.next();
			key = currentEntry.getKey();
			value.set(currentEntry.getValue());
			processedKVs++;
			return true;
		}
		return false;
	}

}
