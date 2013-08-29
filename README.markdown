RDBI
====

RDBI provides a convenience interface for Lua operations with Redis in Java. It uses Jedis as the Redis driver and is not
an abstraction layer ontop of Jedis. It tries to make Lua operations with Jedis simpler and cleaner.

USAGE
-----

	private static interface TestDAO {
		@RedisQuery(
	    	"redis.call('SET',  KEYS[1], ARGV[1]);" +
	        "return 0;"
	    )
	    int testExec(List<String> keys, List<String> args);
	}
	
	...
	
	RDBI rdbi = new RDBI(new JedisPool("localhost"));

	rdbi.withHandle(new RDBICallback<Integer>() {
			@Override
	        public Integer run(JedisHandle handle) {
	        	return handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world"));
	        }
	});


TODO
----

- Decide if private static versus private for RDBIProxyFactory
- Naming of RDBI package protected classes
- use antlr to clean up usage of lua string; ie. I don't like lua script to have to do KEYS[1] and ARGV[1], I'd like it to look like jdbi :myKey, :myValue


