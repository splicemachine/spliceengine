package com.splicemachine.derby.ddl;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public interface CommunicationListener{
    void onCommunicationEvent(String node);
}
