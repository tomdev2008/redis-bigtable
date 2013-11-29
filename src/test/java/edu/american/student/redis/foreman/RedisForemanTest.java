package edu.american.student.redis.foreman;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.american.student.redis.hadoop.RedisBigTableKey;

public class RedisForemanTest
{
	private static RedisForeman foreman = new RedisForeman();

	@BeforeClass
	public static void before() throws Exception
	{
		foreman.deleteTables();
	}

	@Test
	public void connectivityTest()
	{
		assertTrue("Not Connected", foreman.isConnected());
	}

	@Test
	public void createTableTest()
	{
		foreman.createTable("example".getBytes());
		assertTrue("Created Table does not exist", foreman.tableExists("example".getBytes()));
	}

	@Test
	public void addRowsTest() throws Exception
	{
		byte[] table = "example2".getBytes();
		byte[] row = "row2".getBytes();
		byte[] cf = "cf2".getBytes();
		byte[] cq = "cq2".getBytes();
		byte[] value = "value".getBytes();
		foreman.createTable(table);
		foreman.write(table, row, cf, cq, value);
		Entry<RedisBigTableKey, byte[]> returnedValue = foreman.getByQualifier(table, row, cf, cq);
		assertEquals(new String(value), new String(returnedValue.getValue()));
	}

}
