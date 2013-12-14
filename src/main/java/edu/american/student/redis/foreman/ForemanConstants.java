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
package edu.american.student.redis.foreman;

/**
 * A constants enum which will call System.getEnv or System.getProperty
 * @author cam
 *
 */
public class ForemanConstants
{
	public enum RedisConstants
	{
		/**
		 * The redis-server host IP. Jedis connects using this host number
		 */
		REDIS_HOST,
		/**
		 * The redis-server host port. Jedis connects using this port number
		 */
		REDIS_PORT;

		private String value;

		RedisConstants()
		{
			String env = System.getenv(this.name());
			if (env == null)
			{
				env = System.getProperty(this.name());
				if (env == null)
				{
					throw new RuntimeException("Property:" + this.name() + " is not an env or property");
				}
			}
			value = env;
		}

		@Override
		public String toString()
		{
			return value;
		}
	}

	public enum TableIdentifier
	{
		TABLE, ROW;

		private byte[] id;

		TableIdentifier()
		{
			this.id = ("!" + name()).getBytes();
		}

		public byte[] getId()
		{
			return id;
		}

		public static TableIdentifier getIdentifierFromName(byte[] name)
		{
			for (TableIdentifier idd : TableIdentifier.values())
			{
				if (("!" + idd.name()).equals(new String(name)))
				{
					return idd;
				}
			}
			return null;
		}
	}
}
