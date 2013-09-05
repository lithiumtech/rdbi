# Scratching an Itch

Two years ago Social Web started using Redis in order to horizontally scale our session data and running state. The best and most well documented driver then and now for Redis is Jedis. Jedis is nice library with a few [warts](https://github.com/xetorthio/jedis/issues/44).

## A Wart

Jedis uses a thread pool in order reuse open connections to Redis for performance. As an application developer, you grab a Jedis client from the pool, use the client, then return the client to the pool. However if the client loses connection to Redis either from a network or programming error, the client must be cleaned up before returning to the pool. The problem is the Jedis api forces this clean up on the application developer:

	JedisPool pool = new JedisPool("localhost");	
	Jedis jedis = null;
	boolean broken = false; //extra variable

	// The execution block
	try {
		jedis = pool.getResource();
		jedis.set("hello", "world");
	} catch (JedisException e) {
		broken = true; //extra setter
		throw new RuntimeException(e.getMessage(), e);
	} finally {
		if (jedis != null) {
			if (broken) {	//extra if clause
				pool.returnBrokenResource(jedis);
			} else {
				pool.returnResource(jedis);
			}
		}
	}

The variable broken is a new variable that must be introduced, it must be set in each catch block, and an extra if clause is necessary. Every call using Jedis will incur the cost of 7 lines of code. Although it doesn't look that bad now, if you use Jedis extensively, and we do, the tiny warts add up.

## Scratching the Surface

The obvious solution is a callback pattern. That is what LSW chose:

	interface JedisCallback<T> {
		T run(Jedis jedis);
	}

	public class JedisExecutor {

		private JedisPool pool; /* ..from injection.. */

		public <T> T exec(JedisCallback<T> t) {
			//...
			try {
				jedis = pool.getResource();
				return t.run(jedis);
			} catch (JedisException e) {
				//..
			} finally {
				//..
			}
		}
	}

	//...

	final String input = "hello";

	String output = new JedisExecutor().exec(new JedisCallback<String> {
			@Override
			String run(Jedis jedis) {
				jedis.set("somekey", "output");
			}
	});

Cleaner, but notice the final on the input which clearly separates the calling body from the anonymous callback function, the loss of precision for what exceptions we receive, and the cruft for the extra callback. Can we do better? Yes, but before we go further, I have to sate that this pattern has worked pretty well for two years, and before we go jumping off into the deep-end we should keep a cost-benefit analysis in the back of our heads.

## Deep Tissue Scratching

When designing an API, I like to ask myself two questions. First question: how can I make the application develop think less about the technology and more about the business logic? That's easy:

- If a Jedis client comes from a JedisPool and is returned to the JedisPool, why do I have to keep the JedisPool around? Shouldn't the Jedis client know about its pool?
- If a Jedis client looses connection and throws an exception, shouldn't it know about it? Why do I have to know that it is broken or not?

The second question is whose api can I steal? I'm sure they've thought it through. That's easy, in Java 7 we could use [try-with-resource](http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html):

	JedisPool pool = new JedisPool("localhost");	
	
	try(Jedis jedis = pool.getResource();) {
		jedis.set("hello", "world");
	}

After the try block, jedis.close() would be called and it would automagically return to the pool from which it came from and also know if it was broken.




