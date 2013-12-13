package edu.american.student.redis.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.american.student.redis.foreman.RedisForeman;

public class RedisBigTableRecordReader extends RecordReader<RedisBigTableKey, Text>
{

	@Override
	public void close() throws IOException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public RedisBigTableKey getCurrentKey() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		RedisBigTableKey key = RedisBigTableKey.inflate(split.getLocations()[0].getBytes());
		byte[] row = key.getRow();
		byte[] cf = key.getColumnFamily();
		byte[] cq = key.getColumnQualifier();
		RedisForeman foreman = new RedisForeman();
		foreman.connect();
		// Allow the foreman to accept wild card values TODO

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return false;
	}

}
