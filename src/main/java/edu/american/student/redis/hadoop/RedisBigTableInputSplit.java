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
