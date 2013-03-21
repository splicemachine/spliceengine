package com.splicemachine.perf.runner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * @author Scott Fines
 * Created on: 3/18/13
 */
public class JDBCConnectionPool {
    private final Semaphore poolPermits;
    private final BlockingQueue<Connection> connections = new LinkedBlockingQueue<Connection>();
    private final int poolSize;
    private final String serverName;
    private volatile boolean closed = false;

    public JDBCConnectionPool(String serverName,int poolSize) {
        this.poolPermits = new Semaphore(poolSize);
        this.poolSize = poolSize;
        this.serverName = serverName;
    }

    public Connection getConnection() throws Exception {
        if(closed) throw new AssertionError("Pool closed!");
        //try to acquire a permit first
        poolPermits.acquire();

        synchronized(connections){
            //we have a permit--get a Connection from the connections if it's there
            //otherwise, we'll need to create one
            if(connections.isEmpty()){
                return createConnection();
            }
            //get the first connection in the pool
            return connections.poll();
        }
    }

    public void returnConnection(Connection connection) throws SQLException {

        if(closed ||connections.size()>poolSize){
            connection.close();
        }
        if(!closed){
            connections.add(connection);
            poolPermits.release();
        }
    }

    public int availableConnections() {
        return connections.size();
    }

    public void preLoadConnections() throws Exception{
        for(int i=0;i<poolSize;i++){
            Connection connection = createConnection();
            returnConnection(connection);
        }
    }

    public void shutdown() throws Exception{
        closed=true;
        for(Connection connection:connections){
            if(!connection.isClosed()){
                connection.commit();
                connection.close();
            }
        }
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:derby://"+serverName+"/wombat;create=true");
    }

}
