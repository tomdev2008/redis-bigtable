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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *  This input format will read all the data from a given set of Redis hosts 
 * @author cam
 *
 */
public class RedisHashInputFormat extends InputFormat<Text, Text>
{
	/*Again, the CSV list of hosts and a hash key variables and methods for configuration */
	public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

	public static void setRedisHosts(Job job, String hosts)
	{
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
	}

	public static void setRedisHashKey(Job job, String hashKey)
	{
		job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
	}

	/**
	 *  This method will return a list of InputSplit objects.
	 *  The framework uses this to create an equivalent number of map tasks(non-Javadoc)
	 *  @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException
	{
		/* Get our configuration values and ensure they are set */
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty())
		{
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}
		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
		if (hashKey == null || hashKey.isEmpty())
		{
			throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
		}
		/* Create an input split for each Redis instance */
		/* More on this custom split later, just know that one is created per host */
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String host : hosts.split(","))
		{
			splits.add(new RedisHashInputSplit(host, hashKey));
		}
		return splits;
	}

	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		return new RedisHashRecordReader();
	}
}