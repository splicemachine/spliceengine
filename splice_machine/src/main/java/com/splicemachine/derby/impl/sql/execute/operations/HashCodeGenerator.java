package com.splicemachine.derby.impl.sql.execute.operations;

/**
 * @author Scott Fines
 *         Created on: 9/13/13
 */
public interface HashCodeGenerator<K> {

    int hash(K entity);
}
