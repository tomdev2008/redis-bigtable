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

public class MessageFactory extends MFactory
{
	private String objective;
	private String status = "failed";
	private Object[] objects;
	private String issue;

	// objective status issue objects source
	public static StatsFactory objective(String objective)
	{
		MessageFactory factory = new MessageFactory();
		factory.objective = objective;
		return new StatsFactory(factory);
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(objective).append(" ").append(status).append(". ");
		if (issue != null)
		{
			sb.append(issue).append(". ");
		}
		if (objects != null)
		{
			for (Object obj : objects)
			{
				sb.append(obj).append(" ");
			}
		}

		return sb.toString();
	}

	public static class StatsFactory extends MFactory
	{
		private MessageFactory instance;

		public StatsFactory(MessageFactory instance)
		{
			this.instance = instance;
		}

		public IssueFactory ok()
		{
			instance.status = "ok";
			return new IssueFactory(instance);
		}

		public IssueFactory started()
		{
			instance.status = "started";
			return new IssueFactory(instance);
		}

		public IssueFactory stopped()
		{
			instance.status = "stopped";
			return new IssueFactory(instance);
		}

		public IssueFactory completed()
		{
			instance.status = "completed";
			return new IssueFactory(instance);
		}

		public IssueFactory ignored()
		{
			instance.status = "ignored";
			return new IssueFactory(instance);
		}

		public ObjFactory issue(String issue)
		{
			instance.issue = issue;
			return new ObjFactory(instance);
		}

		public SourceFactory objects(Object... objs)
		{
			instance.objects = objs;
			return new SourceFactory(instance);
		}

		@Override
		public String toString()
		{
			return instance.toString();
		}

	}

	public static class IssueFactory extends MFactory
	{
		private MessageFactory instance;

		public IssueFactory(MessageFactory instance)
		{
			this.instance = instance;
		}

		public ObjFactory issue(String issue)
		{
			instance.issue = issue;
			return new ObjFactory(instance);
		}

		public SourceFactory objects(Object... obj)
		{
			instance.objects = obj;
			return new SourceFactory(instance);
		}

		@Override
		public String toString()
		{
			return instance.toString();
		}
	}

	public static class ObjFactory extends MFactory
	{
		private MessageFactory instance;

		public ObjFactory(MessageFactory instance)
		{
			this.instance = instance;
		}

		public SourceFactory objects(Object... objs)
		{
			instance.objects = objs;
			return new SourceFactory(instance);
		}

		@Override
		public String toString()
		{
			return instance.toString();
		}
	}

	public static class SourceFactory extends MFactory
	{
		private MessageFactory instance;

		public SourceFactory(MessageFactory instance)
		{
			this.instance = instance;
		}

		@Override
		public String toString()
		{
			return instance.toString();
		}
	}
}
