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
package edu.american.student.redis;

public class Utils
{

	public static final byte RECORD_SEPARATOR = 0x1E;
	public static final byte WILD_CARD = '*';
	public static final byte[] EMPTY = new byte[] {};

	public static boolean hasWildCard(byte[] bytes)
	{
		if (bytes.length > 0)
		{
			return bytes[0] == Utils.WILD_CARD;
		}
		return false;
	}

	public static boolean byteArraysEqual(byte[] first, byte[] second)
	{
		if (first.length == second.length)
		{
			boolean equals = true;
			for (int i = 0; i < first.length; i++)
			{
				equals = equals && first[i] == second[i];
			}
			return equals;
		}
		return false;
	}
}
