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
		validate(row);
		validate(columnFamily);
		validate(columnQualifier);
		this.row = row;
		this.cf = columnFamily;
		this.cq = columnQualifier;
	}

	public byte[] getRow()
	{
		return row;
	}

	public byte[] getColumnFamily()
	{
		return cf;
	}

	public byte[] getColumnQualifier()
	{
		return cq;
	}

	private void validate(byte[] bytes)
	{
		for (byte b : bytes)
		{
			if (b == Utils.RECORD_SEPARATOR)
			{
				throw new RuntimeException("Key contains a Record Separator 0x1E");
			}
		}

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
				continue;
			}
			else if (!cfFinished && b == Utils.RECORD_SEPARATOR)
			{
				cfFinished = true;
				continue;
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

	@Override
	public String toString()
	{
		return new String(toRedisField());
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

	public boolean matches(RedisBigTableKey k)
	{
		byte[] otherRow = k.getRow();
		byte[] otherFamily = k.getColumnFamily();
		byte[] otherQualifier = k.getColumnQualifier();
		boolean rowMatches = Utils.hasWildCard(otherRow) || Utils.byteArraysEqual(this.row, otherRow);
		boolean famMatches = Utils.hasWildCard(otherFamily) || Utils.byteArraysEqual(this.cf, otherFamily);
		boolean qualMatches = Utils.hasWildCard(otherQualifier) || Utils.byteArraysEqual(this.cq, otherQualifier);
		return rowMatches && famMatches && qualMatches;
	}
}
