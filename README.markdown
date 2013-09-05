# rDBI

rDBI provides convenience utilites for Jedis. rDBI cleans up some of Jedis's api pitfalls by [automatically returning the Jedis resource to the pool](https://github.com/xetorthio/jedis/issues/44). It also has helper utilities for making [Lua script calls to Redis](http://redis.io/commands/eval) easier. rDBI library is inspired by the awesome library [jDBI](http://jdbi.org/), a convenience library for SQL.  

# USAGE
## Cleanup of Jedis

The main cleanup we like to make in Jedis is, if the jedis client came from a pool, it should know to return to that pool appropriately without having the application code keep track of the state of the Jedis client.

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

Jedis provides a basic way of loading a Lua script into Redis and eval the script by its sha1 hash. rDBI provides the same functionality but cleans it up so the application developer does not have to think about preloading the scripts on startup of the app and injecting in a hashmap of sha1 keys where ever they want to use Lua functionality. rDBI will cache the lua scripts internally and load them on demand while keeping it all threadsafe. The api is based off of [jDBI's fluent queries](http://jdbi.org/fluent_queries/).

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
### Presence


TODO
----

- ~~Decide if private static versus private for RDBIProxyFactory~~
- ~~Naming of RDBI package protected classes~~
- ~~use antlr to clean up usage of lua string; ie. I don't like lua script to have to do KEYS[1] and ARGV[1], I'd like it to look like jdbi :myKey, :myValue~~
- ~~see if there's something I can do about the handle.jedis() . The .jedis() part annoys me~~
- ~~performance tests for the cglib usage.
- ~~integration test for the sha1 usage.
- extract recipes.


