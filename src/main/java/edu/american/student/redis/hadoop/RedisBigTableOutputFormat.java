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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.redis.MessageFactory;
import edu.american.student.redis.foreman.RedisForeman;

public class RedisBigTableOutputFormat extends OutputFormat<RedisBigTableKey, Text>
{
	private static byte[] table;

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException
	{
		RedisForeman foreman = new RedisForeman();
		boolean tableExists = foreman.tableExists(RedisBigTableOutputFormat.table);
		if (!tableExists)
		{
			throw new IOException(MessageFactory.objective("Validate output format").issue("Table does not exist").objects(table).toString());
		}

	}

	public static void setTable(byte[] table)
	{
		RedisBigTableOutputFormat.table = table;
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
	{
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	@Override
	public RecordWriter<RedisBigTableKey, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		return new RedisBigTableRecordWriter(table);
	}
}
