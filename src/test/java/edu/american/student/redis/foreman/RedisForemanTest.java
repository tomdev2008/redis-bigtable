package edu.american.student.redis.foreman;

import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

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
	public void writeToTableTest() throws Exception
	{
		RedisForeman foreman = new RedisForeman();
		byte[] table = "example".getBytes();
		foreman.createTable(table);
		assertTrue(foreman.tableExists(table));
		byte[] row = "row".getBytes();
		byte[] cf = "column family".getBytes();
		byte[] cq = "column qualifier".getBytes();
		byte[] value = "value".getBytes();
		foreman.write(table, row, cf, cq, value);
		assertTrue(foreman.rowExists(table, row));
		assertTrue(foreman.columnFamilyExists(table, row, cf));
		assertTrue(foreman.columnQualifierExists(table, row, cf, cq));
		assertTrue(foreman.entryExists(table, row, cf, cq, value));
		foreman.deleteRow(table, row, cf, cq);
		assertTrue(!foreman.rowExists(table, row));
		/*	foreman.write(table, map);
			foreman.write(table, key, value);*/

	}

	//	@Test
	//	public void addRowsTest() throws Exception
	//	{
	//		byte[] table = "example2".getBytes();
	//		byte[] row = "row2".getBytes();
	//		byte[] cf = "cf2".getBytes();
	//		byte[] cq = "cq2".getBytes();
	//		byte[] value = "value".getBytes();
	//		foreman.createTable(table);
	//		foreman.write(table, row, cf, cq, value);
	//		Entry<RedisBigTableKey, byte[]> returnedValue = foreman.getByQualifier(table, row, cf, cq);
	//		assertEquals(new String(value), new String(returnedValue.getValue()));
	//	}

}
