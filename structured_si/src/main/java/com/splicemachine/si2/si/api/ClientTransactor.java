package com.splicemachine.si2.si.api;

import java.util.List;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store).
 */
public interface ClientTransactor {
    void initializePuts(List puts);
}
