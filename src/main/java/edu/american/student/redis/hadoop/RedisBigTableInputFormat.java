package edu.american.student.redis.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RedisBigTableInputFormat extends InputFormat<RedisBigTableKey, Text>
{
	private List<byte[]> rows = new ArrayList<byte[]>();
	private List<byte[]> cfs = new ArrayList<byte[]>();
	private List<byte[]> cqs = new ArrayList<byte[]>();
	private List<byte[]> values = new ArrayList<byte[]>();
	private byte[] table;

	@Override
	public RecordReader<RedisBigTableKey, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException
	{
		return new RedisBigTableRecordReader();
	}

	public void setTable(byte[] table)
	{
		this.table = table;
	}

	public void fetchRows(byte[]... rows)
	{
		for (byte[] r : rows)
		{
			this.rows.add(r);
		}
	}

	public void fetchColumnFamilies(byte[]... cfs)
	{
		for (byte[] cf : cfs)
		{
			this.cfs.add(cf);
		}
	}

	public void fetchColumnQualifiers(byte[]... cqs)
	{
		for (byte[] cq : cqs)
		{
			this.cqs.add(cq);
		}
	}

	public void fetchValues(byte[]... values)
	{
		for (byte[] value : values)
		{
			this.values.add(value);
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException
	{
		List<InputSplit> splits = new ArrayList<InputSplit>();
		if (!rows.isEmpty())
		{
			rows.add(new byte[] { '*' });
		}
		if (!cfs.isEmpty())
		{
			cfs.add(new byte[] { '*' });
		}
		if (!cqs.isEmpty())
		{
			cqs.add(new byte[] { '*' });
		}
		if (!values.isEmpty())
		{
			values.add(new byte[] { '*' });
		}

		for (byte[] r : rows)
		{
			for (byte[] cf : cfs)
			{
				for (byte[] cq : cqs)
				{
					for (byte[] v : values)
					{
						RedisBigTableKey k = new RedisBigTableKey(r, cf, cq);
						splits.add(new RedisBigTableInputSplit(table, k, v));
					}
				}
			}
		}
		return splits;
	}

}
