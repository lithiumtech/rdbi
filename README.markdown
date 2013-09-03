RDBI
====

RDBI provides a convenience interface for Lua operations with Redis in Java. It uses Jedis as the Redis driver and is not
an abstraction layer ontop of Jedis. It tries to make Lua operations with Jedis simpler and cleaner. Finally it cleans some Jedis usage up (why do I have to keep track that it's a broken jedis object?).

Eventually I'd like RDBI to do service management of Redis cluster and automatic failover for the client, either via redis sentinal or zookeeper service pool.

USAGE
-----

	private static interface TestDAO {
		@RedisQuery(
	    	"redis.call('SET',  KEYS[1], ARGV[1]);" +
	        "return 0;"
	    )
	    int testExec(List<String> keys, List<String> args);
	}
	
	//...
	
	RDBI rdbi = new RDBI(new JedisPool("localhost"));

	// use it with a handle to make sure everything is closed out automatically
	rdbi.withHandle(new JedisCallback<Integer>() {
			@Override
	        public Integer run(JedisHandle handle) {
	        	return handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world"));
	        }
	});
	
	//or:
	JedisHandle handle = rdbi.open();
	try {
		handle.jedis().get("a");
		handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world"));
	} finally {
		handle.close();
	}
	
	// Java 7 version:
	try (JedisHandle handle = rdbi.open();) {
		handle.jedis().get("a");
		handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world"));
	}

TODO
----

- ~~Decide if private static versus private for RDBIProxyFactory~~
- ~~Naming of RDBI package protected classes~~
- ~~use antlr to clean up usage of lua string; ie. I don't like lua script to have to do KEYS[1] and ARGV[1], I'd like it to look like jdbi :myKey, :myValue~~
- ~~see if there's something I can do about the handle.jedis() . The .jedis() part annoys me~~
- performance tests for the cglib usage.
- integration test for the sha1 usage.
- decide how RDBI will handle jedis/redis down
	- either zookeeper redis service pool
	- redis sentinal

