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

import java.io.Serializable;

public abstract class SBuilder implements Serializable
{
	private static final long serialVersionUID = 5163169675236148188L;
	protected ScannerBuilder instance;

	public SBuilder()
	{

	}

	public SBuilder(ScannerBuilder sbuild)
	{
		instance = sbuild;
	}

	public ScannerBuilder getInstance()
	{
		return instance;
	}

	@Override
	public String toString()
	{
		return instance.toString();
	}

	public abstract byte[] getTable();

	public abstract byte[][] getRows();

	public abstract byte[][] getFamiles();

	public abstract byte[][] getQualifiers();

}
