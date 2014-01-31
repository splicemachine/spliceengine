package com.splicemachine.hbase.writer;

/**
 * An unfortunate abstraction that will allow us better control
 * over when a particular thread or entity is put to sleep (allows
 * us to remove sleeps for testing and stuff like that).
 *
 * @author Scott Fines
 * Date: 1/31/14
 */
public interface Sleeper {

		void sleep(long wait) throws InterruptedException;

		public static Sleeper THREAD_SLEEPER = new Sleeper() {
				@Override
				public void sleep(long wait) throws InterruptedException {
					Thread.sleep(wait);
				}
		};
}
