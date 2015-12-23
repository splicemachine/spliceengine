package com.splicemachine.pipeline.contextfactory;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface ContextFactoryDriver{

    ContextFactoryLoader getLoader(long conglomerateId);
}
