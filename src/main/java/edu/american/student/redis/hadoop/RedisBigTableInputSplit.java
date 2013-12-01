package edu.american.student.redis.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class RedisBigTableInputSplit extends InputSplit
{

	public RedisBigTableInputSplit(RedisBigTableKey k, byte[] v)
	{
		// TODO Auto-generated constructor stub
	}

	@Override
	public long getLength() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
