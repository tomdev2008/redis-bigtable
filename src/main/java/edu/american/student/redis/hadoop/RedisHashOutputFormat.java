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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * This output format class is templated to accept a key and value of type Text
 * @author cam
 *
 */
public class RedisHashOutputFormat extends OutputFormat<Text, Text>
{
	/*
	 * These static conf variables and methods are used to modify the job configuration.
	 * This is a common pattern for MapReduce related classes to avoid the magic string problem.
	 */
	public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
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
	 * This method returns an instance of a RecordWriter for the task.  
	 * Note how we are pulling the variables set by the static methods during configuration
	 */
	@Override
	public RedisHashRecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
	{
		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
		String csvHosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		return new RedisHashRecordWriter(hashKey, csvHosts);
	}

	@Override
	/**
	 * This method is used on the front-end prior to job submission to ensure everything is configured correctly
	 */
	public void checkOutputSpecs(JobContext job) throws IOException
	{
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
	}

	/**
	 * The output committer is used on the back-end to, well, commit output.
	 */
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
	{
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

}