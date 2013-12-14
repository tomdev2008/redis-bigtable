package edu.american.student.redis.foreman;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.american.student.redis.Utils;
import edu.american.student.redis.hadoop.RedisBigTableKey;

public class RedisForemanTest
{
	//	private static RedisForeman foreman = new RedisForeman();

	@BeforeClass
	public static void before() throws Exception
	{
	}

	@Test
	public void connectionTest()
	{
		RedisForeman foreman = new RedisForeman();
		assertTrue("Not Connected", foreman.isConnected());
		foreman.disconnect();
		assertTrue("RedisForeman is still connected after calling disconnect", !foreman.isConnected());
	}

	@Test
	public void createDeleteTableTest()
	{
		byte[] exampleTable = "example".getBytes();
		RedisForeman foreman = new RedisForeman();
		foreman.createTable(exampleTable);
		assertTrue("Created Table does not exist", foreman.tableExists(exampleTable));
		foreman.deleteTable(exampleTable);
		assertTrue("Removed Table still exists", !foreman.tableExists(exampleTable));
	}

	@Test
	public void deleteTablesTest()
	{
		RedisForeman foreman = new RedisForeman();
		byte[] exampleTable1 = "example".getBytes();
		byte[] exampleTable2 = "example1".getBytes();
		foreman.createTable(exampleTable1);
		foreman.createTable(exampleTable2);
		assertTrue(foreman.tableExists(exampleTable1));
		assertTrue(foreman.tableExists(exampleTable2));
		foreman.deleteTables();
		assertTrue("Table was not removed by deleteTables()", !foreman.tableExists(exampleTable1));
		assertTrue("Table was not removed by deleteTables()", !foreman.tableExists(exampleTable2));

	}

	@Test
	public void rowInstancesTest() throws Exception
	{
		RedisForeman foreman = new RedisForeman();
		foreman.deleteTables();
		byte[] exampleTable1 = "example".getBytes();
		byte[] r = "row".getBytes();
		byte[] cf = "column family".getBytes();
		byte[] cq = "column qualifier".getBytes();
		RedisBigTableKey key = new RedisBigTableKey(r, cf, cq);
		foreman.createTable(exampleTable1);
		foreman.write(exampleTable1, key, Utils.EMPTY);
		int instances = foreman.getInstancesOfRow(r);
		System.out.println(instances);
	}

	@Test
	public void writeToTableTest() throws Exception
	{
		RedisForeman foreman = new RedisForeman();
		foreman.deleteTables();
		byte[] table = "example".getBytes();
		assertTrue(!foreman.tableExists(table));
		foreman.createTable(table);
		assertTrue(foreman.tableExists(table));
		byte[] row = "row".getBytes();
		byte[] cf = "column family".getBytes();
		byte[] cq = "column qualifier".getBytes();
		byte[] value = "value".getBytes();
		foreman.write(table, row, cf, cq, value);
		assertTrue("Foreman could not find row that was just written", foreman.rowExists(table, row));
		assertTrue("Foreman could not found column family that was just written", foreman.columnFamilyExists(table, row, cf));
		assertTrue("Foreman could not find column qualifier that was just written", foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue("Foreman could not find entry that was just written", foreman.entryExists(table, row, cf, cq, value));
		foreman.deleteRow(table, row, cf, cq);
		assertTrue("Foreman found row that was just deleted", !foreman.rowExists(table, row));
		assertTrue("Foreman found column family that was deleted", !foreman.columnFamilyExists(table, row, cf));
		assertTrue("Foreman found column qualifier that was just deleted", !foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue("Foreman found entry that was just deleted", !foreman.entryExists(table, row, cf, cq, value));
		RedisBigTableKey key = new RedisBigTableKey(row, cf, cq);
		foreman.write(table, key, value);
		assertTrue("Foreman could not find row that was just written", foreman.rowExists(table, row));
		assertTrue("Foreman could not found column family that was just written", foreman.columnFamilyExists(table, row, cf));
		assertTrue("Foreman could not find column qualifier that was just written", foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue("Foreman could not find entry that was just written", foreman.entryExists(table, row, cf, cq, value));
		foreman.deleteRow(table, key);
		assertTrue("Foreman found row that was just deleted", !foreman.rowExists(table, row));
		assertTrue("Foreman found column family that was deleted", !foreman.columnFamilyExists(table, row, cf));
		assertTrue("Foreman found column qualifier that was just deleted", !foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue("Foreman found entry that was just deleted", !foreman.entryExists(table, row, cf, cq, value));

		Map<RedisBigTableKey, byte[]> map = new HashMap<RedisBigTableKey, byte[]>();
		map.put(key, value);
		foreman.write(table, map);
		assertTrue("Foreman could not find row that was just written", foreman.rowExists(table, row));
		assertTrue("Foreman could not found column family that was just written", foreman.columnFamilyExists(table, row, cf));
		assertTrue("Foreman could not find column qualifier that was just written", foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue("Foreman could not find entry that was just written", foreman.entryExists(table, row, cf, cq, value));
		foreman.deleteRows(table, map);
		assertTrue("Foreman found row that was just deleted", !foreman.rowExists(table, row));
		assertTrue("Foreman found column family that was deleted", !foreman.columnFamilyExists(table, row, cf));
		assertTrue("Foreman found column qualifier that was just deleted", !foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue("Foreman found entry that was just deleted", !foreman.entryExists(table, row, cf, cq, value));

	}

}
