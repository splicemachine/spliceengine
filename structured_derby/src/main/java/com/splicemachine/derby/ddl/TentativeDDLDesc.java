package com.splicemachine.derby.ddl;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/10/14
 * Time: 10:35 PM
 * To change this template use File | Settings | File Templates.
 */
public interface TentativeDDLDesc {

    long getBaseConglomerateNumber();

    long getConglomerateNumber();
}
