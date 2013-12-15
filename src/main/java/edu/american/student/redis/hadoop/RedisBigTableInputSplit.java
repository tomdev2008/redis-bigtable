package edu.american.student.redis.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class RedisBigTableInputSplit extends InputSplit implements Writable
{
	private RedisBigTableKey key;
	private byte[] value;
	private byte[] table;

	public RedisBigTableInputSplit()
	{

	}

	public RedisBigTableInputSplit(byte[] t, RedisBigTableKey k, byte[] v)
	{
		key = k;
		value = v;
		table = t;
	}

	@Override
	public long getLength() throws IOException, InterruptedException
	{
		return (key.toRedisField().length + value.length);
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException
	{
		return new String[] { new String(key.toRedisField()), new String(table) };
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = RedisBigTableKey.inflate(in.readUTF().getBytes());
		value = in.readUTF().getBytes();
		table = in.readUTF().getBytes();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(new String(key.toRedisField()));
		out.writeUTF(new String(value));
		out.writeUTF(new String(table));

	}

}
