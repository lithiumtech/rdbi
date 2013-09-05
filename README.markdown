# RDBI

RDBI provides a convenience interface for [Lua queries](http://redis.io/commands/eval) with Redis using the Jedis Java driver. rDBI also tries to clean up some of Jedis's api pitfalls. rDBI is not an abstraction framework ontop of Jedis; it is a convenience library for Jedis, inspired by [jDBI](http://jdbi.org/). We even use many of jDBI's constructs!


# USAGE
## Cleanup of Jedis

	// You don't have to know if jedis is broken or which pool it comes from
	// Just close the handle and you're good to go!
	
	RDBI rdbi = new RDBI(new JedisPool("localhost"));
	
	JedisHandle handle = rdbi.open();
	
	try {
		Jedis jedis = handle.jedis();
		jedis.get("bla");
	} finally {
		handle.close();
	}
	
	//If you want to get fancy and use Java 7:
	try (JedisHandle handle = rdbi.open();) {
		handle.jedis().get("a");
	}
	
	//Of course old school callback pattern works:
	rdbi.withHandle(new JedisCallback<Integer>() {
		@Override
	        public Integer run(JedisHandle handle) {
	        	return handle.jedis().get("hello");
	        }
	});


## Now onto Lua and Coolness:

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


TODO
----

- ~~Decide if private static versus private for RDBIProxyFactory~~
- ~~Naming of RDBI package protected classes~~
- ~~use antlr to clean up usage of lua string; ie. I don't like lua script to have to do KEYS[1] and ARGV[1], I'd like it to look like jdbi :myKey, :myValue~~
- ~~see if there's something I can do about the handle.jedis() . The .jedis() part annoys me~~
- performance tests for the cglib usage.
- integration test for the sha1 usage.


