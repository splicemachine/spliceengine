package com.splicemachine.derby.impl.job.coprocessor;

import java.io.IOException;
import java.util.List;

/**
 * Strategy pattern for splitting a single region task into many, based on some
 * features of either the task itself, or the strategy.
 *
 * @author Scott Fines
 * Date: 4/14/14
 */
public interface TaskSplitter {

		List<byte[]> split(RegionTask task, byte[] taskStart,byte[] taskStop) throws IOException;
}
