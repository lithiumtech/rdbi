# rDBI

rDBI provides convenience utilites for Jedis, cleaning up some of Jedis's api pitfalls by [automatically returning the Jedis resource to the pool](https://github.com/xetorthio/jedis/issues/44). It also has helper utilities for making [Lua script calls to Redis](http://redis.io/commands/eval) easier. rDBI library is inspired by the awesome library [jDBI](http://jdbi.org/), a convenience library for SQL.  

# USAGE
## Cleanup of Jedis

The main cleanup we made with Jedis is, if the jedis client came from a pool, it should know to return to that pool without having the application code keep track of the state of the Jedis client or which pool to return to.

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
	rdbi.withHandle(new JedisCallback<String>() {
		@Override
	        public String run(JedisHandle handle) {
	        	return handle.jedis().get("hello");
	        }
	});


## Now onto Lua and Coolness:

Jedis provides a basic way of loading a Lua script into Redis and eval the script by its sha1 hash. rDBI provides this functionality via fluent queries, based off of [jDBI's fluent queries](http://jdbi.org/fluent_queries/). The application developer does not have to think about preloading the scripts on startup of the app or creating enums and storing sha1 in hashmaps. rDBI will cache the lua scripts internally and load them on demand while keeping it all thread-safe.

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
	
## Recipes (TODO, writeup)
### Events
### Job Scheduler
The scheduler recipe designs are based on the concepts defined at https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
### Presence
## Redis Sentinel
## Native support of Sharded Redis Instance


License
=======
RDBI is made available publicly under the terms of Apache License v2.0.  See the [LICENSE](LICENSE) file. 

