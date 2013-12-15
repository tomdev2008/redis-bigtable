/**
 * <br>
 * Licensed under the Apache License, Version 2.0 (the "License");<br>
 * you may not use this file except in compliance with the License.<br>
 * You may obtain a copy of the License at<br>
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0<br>
 * <br>
 * Unless required by applicable law or agreed to in writing, software<br>
 * distributed under the License is distributed on an "AS IS" BASIS,<br>
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.<br>
 * See the License for the specific language governing permissions and<br>
 * limitations under the License.<br>
 */
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

/**
 *  This input format will read all the data from a given set of Redis hosts 
 * @author cam
 *
 */
public class RedisBigTableInputFormat extends InputFormat<RedisBigTableKey, Text>
{
	//A list of rows, cfs cqs, and values requirements to grab. if empty, a wildcard is used
	private static List<byte[]> rows = new ArrayList<byte[]>();
	private static List<byte[]> cfs = new ArrayList<byte[]>();
	private static List<byte[]> cqs = new ArrayList<byte[]>();
	private static List<byte[]> values = new ArrayList<byte[]>();
	//The table to look at
	private static byte[] table;

	@Override
	public RecordReader<RedisBigTableKey, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException
	{
		return new RedisBigTableRecordReader();
	}

	public static void setTable(byte[] table)
	{
		RedisBigTableInputFormat.table = table;
	}

	public static void fetchRows(byte[]... rows)
	{
		RedisBigTableInputFormat.rows.clear();
		for (byte[] r : rows)
		{
			RedisBigTableInputFormat.rows.add(r);
		}
	}

	public static void fetchColumnFamilies(byte[]... cfs)
	{
		RedisBigTableInputFormat.cfs.clear();
		for (byte[] cf : cfs)
		{
			RedisBigTableInputFormat.cfs.add(cf);
		}
	}

	public static void fetchColumnQualifiers(byte[]... cqs)
	{
		RedisBigTableInputFormat.cqs.clear();
		for (byte[] cq : cqs)
		{
			RedisBigTableInputFormat.cqs.add(cq);
		}
	}

	public static void fetchValues(byte[]... values)
	{
		RedisBigTableInputFormat.values.clear();
		for (byte[] value : values)
		{
			RedisBigTableInputFormat.values.add(value);
		}
	}

	/**
	 *  This method will return a list of InputSplit objects.
	 *  The framework uses this to create an equivalent number of map tasks(non-Javadoc)
	 *  @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException
	{
		List<InputSplit> splits = new ArrayList<InputSplit>();
		if (rows.isEmpty())
		{
			rows.add(new byte[] { '*' });
		}
		if (cfs.isEmpty())
		{
			cfs.add(new byte[] { '*' });
		}
		if (cqs.isEmpty())
		{
			cqs.add(new byte[] { '*' });
		}
		if (values.isEmpty())
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
