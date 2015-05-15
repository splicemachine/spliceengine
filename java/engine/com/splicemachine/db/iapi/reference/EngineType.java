package com.splicemachine.db.iapi.reference;

/**
 * Derby engine types. Enumerate different modes the
 * emmbedded engine (JDBC driver, SQL langauge layer and
 * store) can run in. A module can query the monitor to
 * see what type of service is being requested in terms
 * of its engine type and then use that in a decision
 * as to if it is suitable.
 * 
 * @see com.splicemachine.db.iapi.services.monitor.ModuleSupportable
 * @see com.splicemachine.db.iapi.services.monitor.Monitor#isDesiredType(Properties, int)
 * @see com.splicemachine.db.iapi.services.monitor.Monitor#getEngineType(Properties)
 *
 */
public interface EngineType {
    /**
     * Full database engine, the typical configuration.
     */
    int STANDALONE_DB = 0x00000002;
    
    /**
     * Property used to define the type of engine required.
     * If not set defaults to STANDALONE_DB.
     */
    String PROPERTY = "derby.engineType";
}