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

public class ScannerBuilder extends SBuilder
{
	private static final long serialVersionUID = -6660181119962229532L;
	private String table = "TABLE_NOT_DEFINED";
	private byte[][] rows = new byte[][] { { '*' } };
	private byte[][] family = new byte[][] { { '*' } };
	private byte[][] qualifier = new byte[][] { { '*' } };

	private ScannerBuilder(String table)
	{
		instance = this;
		this.table = table;
	}

	public static ScannerBuilder table(String table)
	{
		return new ScannerBuilder(table);
	}

	public ScannerBuilder rows(byte[]... rows)
	{
		this.rows = rows;
		return this;
	}

	public ScannerBuilder family(byte[]... family)
	{
		this.family = family;
		return this;
	}

	public ScannerBuilder qualifier(byte[]... quals)
	{
		this.qualifier = quals;
		return this;
	}

	public byte[][] getRange()
	{
		return rows;
	}

	public byte[][] getColumnQualifier()
	{
		return qualifier;
	}

	@Override
	public byte[] getTable()
	{
		return table.getBytes();
	}

}
