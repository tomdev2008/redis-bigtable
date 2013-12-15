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
