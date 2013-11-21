package edu.american.student.redis.foreman;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.Jedis;
import edu.american.student.redis.Utils;
import edu.american.student.redis.hadoop.RedisBigTableKey;

public class RedisForeman
{
	private Jedis instance;

	public RedisForeman()
	{
		instance = new Jedis(ForemanConstants.REDIS_HOST.toString(), Integer.parseInt(ForemanConstants.REDIS_PORT.toString()));
	}

	public void disconnect()
	{
		instance.disconnect();
	}

	public void createTable(byte[] table)
	{
		if (!tableExists(table))
		{
			instance.hset("TABLE".getBytes(), table, "CREATED".getBytes());
		}
	}

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
	}

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
		}
	}

	public boolean tableExists(byte[] table)
	{
		return instance.hexists("TABLE".getBytes(), table);
	}

	public boolean isConnected()
	{
		return instance != null;
	}

	public void write(byte[] table, RedisBigTableKey key, byte[] value)
	{
		instance.hset(table, key.toRedisField(), value);
	}

	public void write(byte[] table, byte[] row, byte[] cf, byte[] cq, byte[] value)
	{
		write(table, new RedisBigTableKey(row, cf, cq), value);
	}

	public void write(byte[] table, Map<RedisBigTableKey, byte[]> map)
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
		instance.hdel(table, redisBigTableKey.toRedisField());
	}

	public Map<RedisBigTableKey, byte[]> getAll(byte[] table)
	{
		Map<byte[], byte[]> map = instance.hgetAll(table);
		Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
		for (Entry<byte[], byte[]> ent : map.entrySet())
		{
			toReturn.put(RedisBigTableKey.inflate(ent.getKey()), ent.getValue());
		}
		return toReturn;
	}

	public Map<RedisBigTableKey, byte[]> getByRow(byte[] table, byte[] row)
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

	public Map<RedisBigTableKey, byte[]> getByFamily(byte[] table, byte[] row, byte[] columnFamily)
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

	private boolean startsWith(RedisBigTableKey key, String keyIsLike)
	{
		// TODO Auto-generated method stub
		return false;
	}

	private String join(byte recordSeparator, byte[]... parts)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Entry<RedisBigTableKey, byte[]> getByQualifier(byte[] table, byte[] row, byte[] cf, byte[] cq)
	{
		return getByKey(table, new RedisBigTableKey(row, cf, cq));
	}

	public Entry<RedisBigTableKey, byte[]> getByKey(byte[] table, RedisBigTableKey k)
	{
		byte[] val = instance.hget(table, k.toRedisField());
		Map<RedisBigTableKey, byte[]> value = new HashMap<RedisBigTableKey, byte[]>();
		value.put(k, val);
		return value.entrySet().iterator().next();
	}
}
