package edu.american.student.redis.foreman;

public enum ForemanConstants
{
	REDIS_HOST, REDIS_PORT;

	private String value;

	ForemanConstants()
	{
		String env = System.getenv(this.name());
		if(env == null) {
			env = System.getProperty(this.name());
			if(env == null) {
				throw new RuntimeException("Property:"+this.name()+" is not an env or property");
			}
		}
		value = env;
	}

	@Override
	public String toString()
	{
		return value;
	}
}
