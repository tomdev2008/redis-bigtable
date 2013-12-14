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
