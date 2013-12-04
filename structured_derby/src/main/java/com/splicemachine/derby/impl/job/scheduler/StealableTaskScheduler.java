package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.Task;
import com.splicemachine.job.TaskScheduler;

/**
 * @author Scott Fines
 * Date: 12/4/13
 */
public interface StealableTaskScheduler<T extends Task> extends TaskScheduler<T> {

		T steal();
}
