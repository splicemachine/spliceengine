package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.job.Status;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenMBeanConstructorInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.OpenMBeanOperationInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Jeff Cunningham
 *         Date: 12/18/13
 */
public class StatementMetrics extends JobMetrics implements StatementManagement, DynamicMBean {

    private static String[] itemNames = { "statement", "jobid", "taskid", "regionserver", "status" };
    private static String[] itemDescriptions = { "Statement", "Job ID", "Task ID", "Region Server", "Status" };
    private static OpenType[] itemTypes = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING };
    private static String[] indexNames = { "taskid" };
    private static TabularType taskTabularType = null;
    private static CompositeType statementInfoType = null;
    static {
        try {
            statementInfoType = new CompositeType("statementinfo", "Statement Info", itemNames, itemDescriptions, itemTypes);
            taskTabularType = new TabularType("pages", "List of Page Size results", statementInfoType, indexNames);
        } catch (OpenDataException e) {
            throw new RuntimeException(e);
        }
    }
    private TabularDataSupport statementInfos;
    private OpenMBeanInfoSupport PSOMBInfo;
    private Map<String, CompositeDataSupport> jobMap = new ConcurrentHashMap<String, CompositeDataSupport>();

    public StatementMetrics() {
        OpenMBeanAttributeInfoSupport[] attributes =
                new OpenMBeanAttributeInfoSupport[] {
                        new OpenMBeanAttributeInfoSupport( "StatementInfos",
                                "Statement Infos sorted by task ID", taskTabularType,
                                true, false, false) };
        PSOMBInfo = new OpenMBeanInfoSupport(this.getClass().getName(),
                "Statement Information", attributes,
                new OpenMBeanConstructorInfoSupport[0],
                new OpenMBeanOperationInfoSupport[0],
                new MBeanNotificationInfo[0]);
        statementInfos = new TabularDataSupport(taskTabularType);
    }

    public TabularData getStatementInfos() {
        return (TabularData) statementInfos.clone();
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attribute == null) {
            throw new RuntimeOperationsException(
                    new IllegalArgumentException("Attribute name cannot be null"),
                    "Cannot call getAttributeInfo with null attribute name");
        }
        if (attribute.equals("StatementInfos")) {
            return getStatementInfos();
        }
        throw new AttributeNotFoundException("Cannot find " + attribute + " attribute ");
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("No attribute can be set in this MBean");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        if (attributes == null) {
            throw new RuntimeOperationsException(
                    new IllegalArgumentException("attributeNames[] cannot be null"),
                    "Cannot call getAttributes with null attribute names");
        }
        AttributeList resultList = new AttributeList();
        if (attributes.length == 0)
            return resultList;
        for (int i = 0; i < attributes.length; i++) {
            try {
                Object value = getAttribute(attributes[i]);
                resultList.add(new Attribute(attributes[i], value));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return (resultList);
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
        throw new RuntimeOperationsException(
                new IllegalArgumentException("No operations defined for this OpenMBean"),
                    "No operations defined for this OpenMBean");
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return PSOMBInfo;
    }

    // =================================================================================

    @Override
    public void addJob(CoprocessorJob job, String jobPath, Set<RegionTaskControl> tasks) {
        String sql = ((OperationJob)job).getInstructions().getStatement().getSource();
        String jobID = job.getJobId();
        for (RegionTaskControl task : tasks) {
            String regionServer = "unknown";
            // TODO: implement  regionServer = task.getRegionServer();

            String status = "unknown";
            try {
                status = task.getStatus().toString();
            } catch (ExecutionException e) {
                // ignore
            }
            // "statement", "jobid", "taskid", "regionserver", "status"
            Object[] itemValues = new Object[] {sql, jobID, Bytes.toString(task.getTaskId()), regionServer, status};
            try {
                CompositeDataSupport cds = new CompositeDataSupport(statementInfoType, itemNames, itemValues);
                statementInfos.put(cds);
                jobMap.put(jobPath, cds);
            } catch (OpenDataException e) {
                // TODO
                e.printStackTrace();
            }
        }
    }

    @Override
    public void removeJob(CoprocessorJob job, String jobPath, Status finalState) {
        super.jobFinished(finalState);
        statementInfos.remove(jobMap.get(jobPath));
    }
}
