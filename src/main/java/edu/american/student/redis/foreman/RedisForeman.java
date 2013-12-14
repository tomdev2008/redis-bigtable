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
import edu.american.student.redis.MessageFactory;
import edu.american.student.redis.Utils;
import edu.american.student.redis.foreman.ForemanConstants.TableIdentifier;
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
	/*Test: RedisForemanTest.connectionTest*/
	public void connect()
	{
		host = ForemanConstants.RedisConstants.REDIS_HOST.toString();
		port = Integer.parseInt(ForemanConstants.RedisConstants.REDIS_PORT.toString());
		instance = new Jedis(host, port);
		log.info("{} connected {} {}", new Object[] { RedisForeman.class.getSimpleName(), host, port });
	}

	/**
	 * Tells Jedis to disconnect from Redis
	 */
	/*Test: RedisForemanTest.connectionTest*/
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
	/*Test: RedisForemanTest.createDeleteTableTest*/
	public void createTable(byte[] table)
	{
		boolean isSystemTable = ForemanConstants.TableIdentifier.getIdentifierFromName(table) != null;
		boolean tableExists = tableExists(table);
		if (!tableExists && !isSystemTable)
		{
			instance.hset(ForemanConstants.TableIdentifier.TABLE.getId(), table, ForemanConstants.TableIdentifierPart.CREATED.getId());
		}
		else if (isSystemTable)
		{
			log.warn("Create table ignored. Table {} is a reserved system table", new String(table));
		}
		else if (tableExists)
		{
			log.warn("Create table ignored. Table {} exists", new String(table));
		}
	}

	/**
	 * Delete a table.
	 * @param table the table to delete
	 */
	/*Test: RedisForemanTest.createDeleteTableTest*/
	public void deleteTable(byte[] table)
	{
		boolean isSystemTable = ForemanConstants.TableIdentifier.getIdentifierFromName(table) != null;
		if (tableExists(table) && !isSystemTable)
		{
			instance.hdel(ForemanConstants.TableIdentifier.TABLE.getId(), table);
			Set<byte[]> keys = instance.hkeys(table);
			for (byte[] key : keys)
			{
				instance.hdel(table, key);
			}
		}
		else if (isSystemTable)
		{
			log.warn("Delete table ignored. Table{} is a system table.", new String(table));
		}
		else
		{
			log.warn("Delete table ignored. Table {} does not exist", new String(table));
		}
	}

	/**
	 * Deletes all tables.
	 * @throws RedisForemanException 
	 */
	/*Test: RedisForemanTest.deleteTablesTest*/
	public void deleteTables() throws RedisForemanException
	{
		Set<byte[]> keys = instance.hkeys(ForemanConstants.TableIdentifier.TABLE.getId());
		for (byte[] key : keys)
		{
			Set<byte[]> subkeys = instance.hkeys(key);
			for (byte[] subkey : subkeys)
			{
				instance.hdel(key, subkey);
			}
			boolean isSystemTable = ForemanConstants.TableIdentifier.getIdentifierFromName(key) != null;
			if (!isSystemTable)
			{
				instance.hdel(ForemanConstants.TableIdentifier.TABLE.getId(), key);
				log.info("Table {} deleted", new String(key));
			}
		}
		clearSystemTables();
	}

	/**
	 * Checks Redis for table existence
	 * @param table the table to check
	 * @return
	 */
	/*Test: RedisForemanTest.createDeleteTableTest*/
	public boolean tableExists(byte[] table)
	{
		boolean isSystemTable = ForemanConstants.TableIdentifier.getIdentifierFromName(table) != null;
		if (!isSystemTable)
		{
			return instance.hexists(ForemanConstants.TableIdentifier.TABLE.getId(), table);
		}
		return true;
	}

	/**
	 * Checks if the Foreman is still connected to Redis
	 * @return
	 */
	/*Test: RedisForemanTest.connectionTest*/
	public boolean isConnected()
	{
		return instance != null;
	}

	/**
	 * Writes an entry to Redis in the form (table, row, column family, column qualifier, value)
	 * @param table The table to write the entry
	 * @param key The entry's identifiers
	 * @param value The entry's value
	 * @throws RedisForemanException Writing to table that does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public void write(byte[] table, RedisBigTableKey key, byte[] value) throws RedisForemanException
	{
		boolean hasWildCard = hasWildCard(key);
		boolean hasEmptyParts = hasEmptyParts(key);
		boolean tableExists = tableExists(table);
		if (tableExists && !hasWildCard && !hasEmptyParts)
		{
			instance.hset(table, key.toRedisField(), value);
			incrementRow(key.getRow());
		}
		else if (!tableExists)
		{
			throw new RedisForemanException(MessageFactory.objective("Write entry").issue("Table does not exist").objects(new String(table)));
		}
		else if (hasWildCard)
		{
			throw new RedisForemanException(MessageFactory.objective("Write entry").issue("Given key has a wildcard").objects(key, Utils.WILD_CARD));
		}
		else if (hasEmptyParts)
		{
			throw new RedisForemanException(MessageFactory.objective("Write entry").issue("Given key has empty parts.").objects(key));
		}
	}

	/**
	 * Writes an entry to Redis in the form (table, row, column family, column qualifier, value)
	 * @param table The table to write the entry to
	 * @param row the entry's row identifier
	 * @param cf the entry's column family identifier
	 * @param cq the entry's column qualifier identifier
	 * @param value the entry's value
	 * @throws RedisForemanException Writing to a table that does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public void write(byte[] table, byte[] row, byte[] cf, byte[] cq, byte[] value) throws RedisForemanException
	{
		write(table, new RedisBigTableKey(row, cf, cq), value);
	}

	/**
	 * Writes an entry to Redis in the form (table, row, column family, column qualifier, value)
	 * @param table The table to write an entry to
	 * @param map A map of keys and their respective values
	 * @throws RedisForemanException writing to a table that does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public void write(byte[] table, Map<RedisBigTableKey, byte[]> map) throws RedisForemanException
	{
		for (Entry<RedisBigTableKey, byte[]> ent : map.entrySet())
		{
			write(table, ent.getKey(), ent.getValue());
		}
	}

	/**
	 * Removes a Entry
	 * @param table The table where the entry is
	 * @param row The row identifier of the entry to delete
	 * @param columnFamily The column family identifier of the entry to delete
	 * @param columnQualifier The column qualifier identifier of the entry to delete
	 * @throws RedisForemanException 
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public void deleteRow(byte[] table, byte[] row, byte[] columnFamily, byte[] columnQualifier) throws RedisForemanException
	{
		deleteRow(table, new RedisBigTableKey(row, columnFamily, columnQualifier));
	}

	/**
	 * Removes a Entry
	 * @param table table where the entry to delete is
	 * @param redisBigTableKey The key to delete
	 * @throws RedisForemanException 
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public void deleteRow(byte[] table, RedisBigTableKey redisBigTableKey) throws RedisForemanException
	{
		boolean tableExists = tableExists(table);
		boolean emptyParts = hasEmptyParts(redisBigTableKey);
		if (tableExists && !emptyParts)
		{
			instance.hdel(table, redisBigTableKey.toRedisField());
			decrementRow(redisBigTableKey.getRow());
		}
		else if (emptyParts)
		{
			log.warn("Delete Row failed. Key has empty parts {}", new String(redisBigTableKey.toRedisField()));
		}
		else if (!tableExists)
		{
			log.warn("Delete Row failed. Table {} does not exist.", new String(table));
		}
	}

	/**
	 * Returns an entire table's entries.  May throw Heap Space exceptions for large tables!
	 * @param table the table to grab entries from
	 * @return
	 * @throws RedisForemanException table does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
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
			throw new RedisForemanException(MessageFactory.objective("Grab all entries").issue("Table does not exist").objects(new String(table)));
		}
	}

	//TODO: getByRow is grabbing all entries and then filtering, better internal structure is needed to remedy
	/**
	 *  Returns all entries under a row identifier
	 * @param table table where the row is located
	 * @param row row to grab entries from
	 * @return
	 * @throws RedisForemanException table does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public Map<RedisBigTableKey, byte[]> getByRow(byte[] table, byte[] row) throws RedisForemanException
	{
		Map<RedisBigTableKey, byte[]> all = getAll(table);
		boolean rowIsWildCard = Utils.hasWildCard(row);
		String keyIsLike = join(Utils.RECORD_SEPARATOR, row);
		Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
		for (Entry<RedisBigTableKey, byte[]> ent : all.entrySet())
		{
			if (rowIsWildCard || startsWith(ent.getKey(), keyIsLike))
			{
				toReturn.put(ent.getKey(), ent.getValue());
			}
		}
		return toReturn;
	}

	//TODO: getByFamily is grabbing all entries and then filtering, better internal structure is needed to remedy
	/**
	 * Returns all entries under a row and column family identifier
	 * @param table table where the row and family are
	 * @param row row where to grab entries from
	 * @param columnFamily columnFamily under that row to grab entries from
	 * @return
	 * @throws RedisForemanException table does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public Map<RedisBigTableKey, byte[]> getByFamily(byte[] table, byte[] row, byte[] columnFamily) throws RedisForemanException
	{
		Map<RedisBigTableKey, byte[]> all = this.getByRow(table, row);
		boolean cfHasWildCard = Utils.hasWildCard(columnFamily);
		String keyIsLike = join(Utils.RECORD_SEPARATOR, row, columnFamily);
		Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
		for (Entry<RedisBigTableKey, byte[]> ent : all.entrySet())
		{
			if (cfHasWildCard || startsWith(ent.getKey(), keyIsLike))
			{
				toReturn.put(ent.getKey(), ent.getValue());
			}
		}
		return toReturn;
	}

	//TODO: getByQualifier is grabbing all entries and then filtering, better internal structure is needed to remedy
	/**
	 * Returns all entries under a row, column family, and column qualifier identifier
	 * @param table table where the row, cf, and cq are
	 * @param row the row where to grab entries from
	 * @param cf the column family under that row to grab entries from
	 * @param cq the column qualifier under the column family to grab entries from
	 * @return
	 * @throws RedisForemanException table does not exist
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public Map<RedisBigTableKey, byte[]> getByQualifier(byte[] table, byte[] row, byte[] cf, byte[] cq) throws RedisForemanException
	{
		boolean hasWildCard = Utils.hasWildCard(cq);
		if (hasWildCard)
		{
			return getByFamily(table, row, cf);
		}
		Map<RedisBigTableKey, byte[]> entries = getByKey(table, new RedisBigTableKey(row, cf, cq));
		return entries;
	}

	/**
	 * Returns all entries under a row, column family, and column qualifier identifier
	 * @param table table table where the row, cf, and cq are
	 * @param k key where the value is
	 * @return
	 * @throws RedisForemanException
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public Map<RedisBigTableKey, byte[]> getByKey(byte[] table, RedisBigTableKey k) throws RedisForemanException
	{
		boolean tableExists = tableExists(table);
		boolean emptyParts = hasEmptyParts(k);
		if (tableExists && !emptyParts)
		{
			Map<RedisBigTableKey, byte[]> toReturn = new HashMap<RedisBigTableKey, byte[]>();
			Set<byte[]> redisKeys = instance.hkeys(table);
			for (byte[] value : redisKeys)
			{
				RedisBigTableKey inflated = RedisBigTableKey.inflate(value);
				if (inflated.matches(k))
				{
					byte[] val = instance.hget(table, inflated.toRedisField());
					if (val != null)
					{
						toReturn.put(inflated, val);
					}
				}
			}
			return toReturn;
		}
		else if (emptyParts)
		{
			throw new RedisForemanException(MessageFactory.objective("Get entry by key").issue("Key has empty parts").objects(k));
		}
		else
		{
			throw new RedisForemanException(MessageFactory.objective("Get entry by key").issue("Table does not exist").objects(new String(table)));
		}
	}

	/**
	 * Checks if an entry exists
	 * @param table
	 * @param row
	 * @param cf
	 * @param cq
	 * @param value
	 * @return
	 * @throws RedisForemanException
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public boolean entryExists(byte[] table, byte[] row, byte[] cf, byte[] cq, byte[] value) throws RedisForemanException
	{
		Map<RedisBigTableKey, byte[]> entries = getByQualifier(table, row, cf, cq);
		for (Entry<RedisBigTableKey, byte[]> ent : entries.entrySet())
		{
			if (ent.getValue().length == value.length)
			{
				boolean equals = true;
				for (int i = 0; i < ent.getValue().length; i++)
				{
					byte b1 = ent.getValue()[i];
					byte b2 = value[i];
					equals = equals && (b1 == b2);
					if (equals)
					{
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Checks to see if  TABLE,ROW,CF,CQ entries exist
	 * @param table
	 * @param row
	 * @param cf
	 * @param cq
	 * @return
	 * @throws RedisForemanException
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public boolean columnQualifierExists(byte[] table, byte[] row, byte[] cf, byte[] cq) throws RedisForemanException
	{
		return !getByQualifier(table, row, cf, cq).isEmpty();
	}

	/**
	 * Checks to see if TABLE,ROW,CF entries exist
	 * @param table
	 * @param row
	 * @param cf
	 * @return
	 * @throws RedisForemanException
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public boolean columnFamilyExists(byte[] table, byte[] row, byte[] cf) throws RedisForemanException
	{
		return !getByFamily(table, row, cf).isEmpty();
	}

	/**
	 * CHecks to see if TABLE,ROW entries exist
	 * @param table
	 * @param row
	 * @return
	 * @throws RedisForemanException
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public boolean rowExists(byte[] table, byte[] row) throws RedisForemanException
	{
		return !getByRow(table, row).isEmpty();
	}

	/**
	 * Removes rows 
	 * @param table
	 * @param map
	 * @throws RedisForemanException 
	 */
	/*Test: RedisForemanTest.writeToTableTest*/
	public void deleteRows(byte[] table, Map<RedisBigTableKey, byte[]> map) throws RedisForemanException
	{
		for (Entry<RedisBigTableKey, byte[]> ent : map.entrySet())
		{
			deleteRow(table, ent.getKey());
		}
	}

	/**
	 * Returns a count of how many times that row appears in all tables
	 * @param row
	 * @return
	 * @throws RedisForemanException
	 */
	/*Test: RedisForemanTest.rowInstancesTest*/
	public int getInstancesOfRow(byte[] row) throws RedisForemanException
	{
		byte[] r = ForemanConstants.RowIdentifierPart.ROW_LOG.getId();
		byte[] cf = row;
		byte[] cq = row;
		RedisBigTableKey rowKey = new RedisBigTableKey(r, cf, cq);
		Map<RedisBigTableKey, byte[]> returned = getByKey(ForemanConstants.TableIdentifier.ROW.getId(), rowKey);
		if (returned.isEmpty())
		{
			return 0;
		}
		else
		{
			int instancesOfRow = Integer.parseInt(new String(returned.entrySet().iterator().next().getValue()));
			return instancesOfRow;
		}
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
			sb.append(new String(part));
			sb.append((char) recordSeparator);
		}
		return sb.toString().replaceAll(recordSeparator + "$", "");
	}

	private boolean hasWildCard(RedisBigTableKey key)
	{
		boolean hasWildCard = Utils.hasWildCard(key.getRow());
		hasWildCard = hasWildCard && Utils.hasWildCard(key.getColumnFamily());
		hasWildCard = hasWildCard && Utils.hasWildCard(key.getColumnQualifier());
		return hasWildCard;
	}

	private boolean hasEmptyParts(RedisBigTableKey key)
	{
		boolean emptyParts = key.getRow().length == 0;
		emptyParts = emptyParts && key.getColumnFamily().length == 0;
		emptyParts = emptyParts && key.getColumnQualifier().length == 0;
		return emptyParts;
	}

	private void incrementRow(byte[] row) throws RedisForemanException
	{
		byte[] r = ForemanConstants.RowIdentifierPart.ROW_LOG.getId();
		byte[] cf = row;
		byte[] cq = row;
		RedisBigTableKey rowKey = new RedisBigTableKey(r, cf, cq);
		Map<RedisBigTableKey, byte[]> returned = getByKey(ForemanConstants.TableIdentifier.ROW.getId(), rowKey);
		if (returned.isEmpty())
		{
			instance.hset(ForemanConstants.TableIdentifier.ROW.getId(), rowKey.toRedisField(), "1".getBytes());
		}
		else
		{
			int instanceOfRow = Integer.parseInt(new String(returned.entrySet().iterator().next().getValue()));
			instanceOfRow++;
			instance.hset(ForemanConstants.TableIdentifier.ROW.getId(), rowKey.toRedisField(), (instanceOfRow + "").getBytes());
		}
	}

	private void decrementRow(byte[] row) throws RedisForemanException
	{
		int instancesOfRow = this.getInstancesOfRow(row);
		instancesOfRow--;
		setRowInstanceCount(row, instancesOfRow);
	}

	private void setRowInstanceCount(byte[] row, int instancesOfRow)
	{
		byte[] r = ForemanConstants.RowIdentifierPart.ROW_LOG.getId();
		byte[] cf = row;
		byte[] cq = row;
		RedisBigTableKey rowKey = new RedisBigTableKey(r, cf, cq);
		if (instancesOfRow > 0)
		{
			instance.hset(ForemanConstants.TableIdentifier.ROW.getId(), rowKey.toRedisField(), (instancesOfRow + "").getBytes());
		}
		else
		{
			instance.hdel(ForemanConstants.TableIdentifier.ROW.getId(), rowKey.toRedisField());
		}

	}

	private void clearSystemTables() throws RedisForemanException
	{
		for (TableIdentifier idd : TableIdentifier.values())
		{
			if (!idd.equals(TableIdentifier.TABLE))
			{
				Map<RedisBigTableKey, byte[]> entries = getAll(idd.getId());
				for (Entry<RedisBigTableKey, byte[]> entry : entries.entrySet())
				{
					deleteRow(idd.getId(), entry.getKey());
				}
			}
		}
	}
}
