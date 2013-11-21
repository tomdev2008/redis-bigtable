package edu.american.student.redis.hadoop;

import java.util.ArrayList;
import java.util.List;

import edu.american.student.redis.Utils;

public class RedisBigTableKey
{
	private byte[] row;
	private byte[] cf;
	private byte[] cq;

	public RedisBigTableKey(byte[] row, byte[] columnFamily, byte[] columnQualifier)
	{
		this.row = row;
		this.cf = columnFamily;
		this.cq = columnQualifier;
	}

	public byte[] toRedisField()
	{
		byte[] toReturn = new byte[row.length + cf.length + cq.length + 2];
		int i = 0;
		for (byte b : row)
		{
			toReturn[i] = b;
			i++;
		}
		toReturn[i] = Utils.RECORD_SEPARATOR;
		i++;
		for (byte b : cf)
		{
			toReturn[i] = b;
			i++;
		}
		toReturn[i] = Utils.RECORD_SEPARATOR;
		i++;
		for (byte b : cq)
		{
			toReturn[i] = b;
			i++;
		}
		return toReturn;
	}

	public static RedisBigTableKey inflate(byte[] inflate)
	{
		boolean rowFinished = false;
		boolean cfFinished = false;
		List<Byte> rowBytes = new ArrayList<Byte>();
		List<Byte> cfBytes = new ArrayList<Byte>();
		List<Byte> cqBytes = new ArrayList<Byte>();
		for (byte b : inflate)
		{
			if (!rowFinished && b == Utils.RECORD_SEPARATOR)
			{
				rowFinished = true;
			}
			else if (!cfFinished && b == Utils.RECORD_SEPARATOR)
			{
				cfFinished = true;
			}

			if (!rowFinished)
			{
				rowBytes.add(b);
			}
			else if (!cfFinished)
			{
				cfBytes.add(b);
			}
			else
			{
				cqBytes.add(b);
			}
		}
		byte[] row = toPrim(rowBytes);
		byte[] cf = toPrim(cfBytes);
		byte[] cq = toPrim(cqBytes);
		return new RedisBigTableKey(row, cf, cq);
	}

	private static byte[] toPrim(List<Byte> bytes)
	{
		byte[] toReturn = new byte[bytes.size()];
		for (int i = 0; i < toReturn.length; i++)
		{
			toReturn[i] = bytes.get(i);
		}
		return toReturn;
	}
}
