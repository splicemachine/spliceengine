package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.job.Status;
import java.util.Set;
import javax.management.MXBean;
/**
 * @author Jeff Cunningham
 *         Date: 12/18/13
 */
@MXBean
public interface StatementManagement {

    void addJob(CoprocessorJob job, String jobPath, Set<RegionTaskControl> tasksToWatch);

    void removeJob(CoprocessorJob job, String jobPath, Status finalState);
}
