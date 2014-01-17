package com.splicemachine.stats;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * @author Scott Fines
 *         Date: 1/16/14
 */
public class UserTimer extends BaseTimer{
		private final ThreadMXBean threadMXBean;

		public UserTimer() {
				this.threadMXBean = ManagementFactory.getThreadMXBean();
		}

		@Override
		protected long getTime() {
				return threadMXBean.getCurrentThreadUserTime();
		}
}
