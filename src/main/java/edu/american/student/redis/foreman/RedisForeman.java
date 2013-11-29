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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import edu.american.student.redis.Utils;
import edu.american.student.redis.hadoop.RedisBigTableKey;

/**
 *  A table operations manager for Redis. It will also enforce big-table constraints.
 * @author cam
 *
 */
public class RedisForeman
{
	private Jedis instance;
	private String host;
	private int port;
	private Logger log = LoggerFactory.getLogger(RedisForeman.class);

	public RedisForeman()
	{
		connect();
	}

	/**
	 * Creates (or reinstates) a connection to Redis
	 */
	public void connect()
	{
		host = ForemanConstants.REDIS_HOST.toString();
		port = Integer.parseInt(ForemanConstants.REDIS_PORT.toString());
		instance = new Jedis(host, port);
		log.info("{} connected {} {}", new Object[] { RedisForeman.class.getSimpleName(), host, port });
	}

	/**
	 * Tells Jedis to disconnect from Redis
	 */
	public void disconnect()
	{
		instance.disconnect();
		instance = null;
		log.info("{} disconnected {} {}", new Object[] { RedisForeman.class.getSimpleName(), host, port });
	}

	/**
	 * Create a table. 
	 * @param table the table name
	 */
	public void createTable(byte[] table)
	{
		if (!tableExists(table))
		{
			instance.hset("TABLE".getBytes(), table, "CREATED".getBytes());
		}
		else
		{
			log.warn("Create table ignored. Table {} exists", new String(table));
		}
	}

	/**
	 * Delete a table.
	 * @param table the table to delete
	 */
	public void deleteTable(byte[] table)
	{
		if (tableExists(table))
		{
			instance.hdel("TABLE".getBytes(), table);
			Set<byte[]> keys = instance.hkeys(table);
			for (byte[] key : keys)
			{
				instance.hdel(table, key);
			}
		}
		else
		{
			log.warn("Delete table ignored. Table {} does not exist", new String(table));
		}
	}

	/**
	 * Deletes all tables.
	 */
	public void deleteTables()
	{
		Set<byte[]> keys = instance.hkeys("TABLE".getBytes());
		for (byte[] key : keys)
		{
			Set<byte[]> subkeys = instance.hkeys(key);
			for (byte[] subkey : subkeys)
			{
				instance.hdel(key, subkey);
			}
			instance.hdel("TABLE".getBytes(), key);
			log.info("Table {} deleted", new String(key));
		}
	}

	/**
	 * Checks Redis for table existence
	 * @param table the table to check
	 * @return
	 */
	public boolean tableExists(byte[] table)
	{
		return instance.hexists("TABLE".getBytes(), table);
	}

	/**
	 * Checks if the Foreman is still connected to Redis
	 * @return
	 */
	public boolean isConnected()
	{
		return instance != null;
	}

	public void write(byte[] table, RedisBigTableKey key, byte[] value) throws RedisForemanException
	{
		if (tableExists(table))
		{
			instance.hset(table, key.toRedisField(), value);
		}
		else
		{
			throw new RedisForemanException("Write failed. Table " + new String(table) + " does not exist");
		}
	}

	public void write(byte[] table, byte[] row, byte[] cf, byte[] cq, byte[] value) throws RedisForemanException
	{
		write(table, new RedisBigTableKey(row, cf, cq), value);
	}

	public void write(byte[] table, Map<RedisBigTableKey, byte[]> map) throws RedisForemanException
	{
		for (Entry<RedisBigTableKey, byte[]> ent : map.entrySet())
		{
			write(table, ent.getKey(), ent.getValue());
		}
	}

	public void deleteRow(byte[] table, byte[] row, byte[] columnFamily, byte[] columnQualifier)
	{
		deleteRow(table, new RedisBigTableKey(row, columnFamily, columnQualifier));
	}

	private void deleteRow(byte[] table, RedisBigTableKey redisBigTableKey)
	{
		if (tableExists(table))
		{
			instance.hdel(table, redisBigTableKey.toRedisField());
		}
		else
		{
			log.warn("Delete Row failed. Table {} does not exist.", new String(table));
		}
	}

	public Map<RedisBigTableKey, byte[]> getAll(byte[] table) throws RedisForemanException
	{
		if (tableExists(table))
		{
			Map<byte[], byte[]> map = instance.hgetAll(table);
			Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
			for (Entry<byte[], byte[]> ent : map.entrySet())
			{
				toReturn.put(RedisBigTableKey.inflate(ent.getKey()), ent.getValue());
			}
			return toReturn;
		}
		else
		{
			throw new RedisForemanException("Grab all entries failed. Table " + new String(table) + " does not exist");
		}
	}

	public Map<RedisBigTableKey, byte[]> getByRow(byte[] table, byte[] row) throws RedisForemanException
	{
		Map<RedisBigTableKey, byte[]> all = getAll(table);
		String keyIsLike = join(Utils.RECORD_SEPARATOR, row);
		Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
		for (Entry<RedisBigTableKey, byte[]> ent : all.entrySet())
		{
			if (startsWith(ent.getKey(), keyIsLike))
			{
				toReturn.put(ent.getKey(), ent.getValue());
			}
		}
		return toReturn;
	}

	public Map<RedisBigTableKey, byte[]> getByFamily(byte[] table, byte[] row, byte[] columnFamily) throws RedisForemanException
	{
		Map<RedisBigTableKey, byte[]> all = getAll(table);
		String keyIsLike = join(Utils.RECORD_SEPARATOR, row, columnFamily);
		Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
		for (Entry<RedisBigTableKey, byte[]> ent : all.entrySet())
		{
			if (startsWith(ent.getKey(), keyIsLike))
			{
				toReturn.put(ent.getKey(), ent.getValue());
			}
		}
		return toReturn;
	}

	public Entry<RedisBigTableKey, byte[]> getByQualifier(byte[] table, byte[] row, byte[] cf, byte[] cq) throws RedisForemanException
	{
		return getByKey(table, new RedisBigTableKey(row, cf, cq));
	}

	public Entry<RedisBigTableKey, byte[]> getByKey(byte[] table, RedisBigTableKey k) throws RedisForemanException
	{
		if (tableExists(table))
		{
			byte[] val = instance.hget(table, k.toRedisField());
			Map<RedisBigTableKey, byte[]> value = new HashMap<RedisBigTableKey, byte[]>();
			value.put(k, val);
			return value.entrySet().iterator().next();
		}
		throw new RedisForemanException("Get Entry by Key failed. Table " + new String(table) + " does not exist");
	}

	private boolean startsWith(RedisBigTableKey key, String keyIsLike)
	{
		return key.toString().startsWith(keyIsLike);
	}

	private String join(byte recordSeparator, byte[]... parts)
	{
		StringBuilder sb = new StringBuilder();
		for (byte[] part : parts)
		{
			for (byte b : part)
			{
				sb.append(b);
			}
			sb.append(recordSeparator);
		}
		return sb.toString().replaceAll(String.valueOf(recordSeparator) + "$", "");
	}

}
