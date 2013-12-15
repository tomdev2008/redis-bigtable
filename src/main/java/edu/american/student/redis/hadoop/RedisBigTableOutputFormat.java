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
