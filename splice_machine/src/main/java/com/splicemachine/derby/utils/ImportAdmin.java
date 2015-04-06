package com.splicemachine.derby.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;

import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.load.ImportTaskManagement;
import com.splicemachine.hbase.jmx.JMXUtils;

/**
 * Administrative class to manage information related to imports such as import tasks running on region servers.
 *
 * @author dwinters
 */
public class ImportAdmin extends BaseAdminProcedures {
	private List<Pair<String, JMXConnector>> connections;

	/**
	 * Constructor that creates JMX connections for all of the region servers and stores them as instance variables.
	 * The caller must remember to call the <em>close</em> method when finished with this object to close the connections.
	 *
	 * @param serverNames
	 * @throws IOException 
	 * @throws SQLException
	 */
	public ImportAdmin() throws IOException, SQLException {
		this.connections = getConnections(SpliceUtils.getServers());
	}

	/**
	 * This method is essentially the "deconstructor" for this instance.
	 * All JMX connections are closed for this instance when this method is called.
	 */
	public void close() {
		super.close(this.connections);
	}

    /**
     * Returns a list of JMX MBeans with statistics for the running import tasks on all of the region servers.
     * Each entry in the list contains a "pair" which contains the region server that is executing the import task
     * and the statistics for the import task.
     *
     * @return list of JMX MBeans with statistics for the running import tasks on all of the region servers
     *
     * @throws SQLException
     */
    public List<Pair<String, ImportTaskManagement>> getRegionServerImportTaskInfo() throws SQLException {
    	final List<Pair<String, ImportTaskManagement>> importTasks = Lists.newArrayList();
    	operateWithExistingConnections(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                importTasks.addAll(JMXUtils.getImportTaskManagement(connections));
            }
        }, this.connections);
		return importTasks;
    }
}
