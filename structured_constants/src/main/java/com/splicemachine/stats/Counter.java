package com.splicemachine.stats;

/**
 * Represents a basic counter.
 *
 * We could of course just use longs directly, but then we'd have to
 * do boolean checks every time to determine if we should collect. This sucks,
 * when we could just do that boolean check once and use polymorphism to do
 * No-ops when we don't need to count.
 *
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface Counter {

		public void add(long value);

		public long getTotal();

		boolean isActive();
}
