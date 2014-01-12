/**
 * Copyright 2013 Cameron Cook
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.american.student.redis.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.american.student.redis.MessageFactory;
import edu.american.student.redis.foreman.RedisForeman;
import edu.american.student.redis.foreman.RedisForemanException;

public class RedisBigTableRecordWriter extends RecordWriter<RedisBigTableKey, Text>
{
	private static byte[] table;
	private RedisForeman foreman = new RedisForeman();

	public RedisBigTableRecordWriter(byte[] table)
	{
		RedisBigTableRecordWriter.table = table;
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		foreman.disconnect();
	}

	@Override
	public void write(RedisBigTableKey key, Text value) throws IOException, InterruptedException
	{
		try
		{
			foreman.write(table, key, value.getBytes());
		}
		catch (RedisForemanException e)
		{
			throw new IOException(MessageFactory.objective("Write key/value").objects(key, value).toString(), e);
		}
	}

}
