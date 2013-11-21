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

public class RedisHashInputSplit extends InputSplit implements Writable
{
	private String location = null;
	private String hashKey = null;

	public RedisHashInputSplit()
	{

	}

	public RedisHashInputSplit(String redisHost, String hash)
	{
		this.location = redisHost;
		this.hashKey = hash;
	}

	public String getHashKey()
	{
		return this.hashKey;
	}

	public void readFields(DataInput in) throws IOException
	{
		this.location = in.readUTF();
		this.hashKey = in.readUTF();
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(location);
		out.writeUTF(hashKey);
	}

	@Override
	public long getLength() throws IOException, InterruptedException
	{
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException
	{
		return new String[] { location };
	}

}