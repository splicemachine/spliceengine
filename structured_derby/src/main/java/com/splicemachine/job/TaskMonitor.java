package com.splicemachine.job;

import javax.management.MXBean;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
@MXBean
public interface TaskMonitor {

    int getNumRegions(String tableId);

    List<String> getRegions(String tableId);

    List<String> getTables();

    List<String> getTasks(String tableId, String regionId);

    void cancelTask(String tableId, String regionId, String taskId);

    String getStatus(String tableId, String regionId, String taskId);
}
