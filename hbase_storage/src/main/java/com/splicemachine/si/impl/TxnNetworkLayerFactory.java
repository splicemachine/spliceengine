package com.splicemachine.si.impl;

import com.splicemachine.access.api.SConfiguration;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface TxnNetworkLayerFactory{

    void configure(SConfiguration configuration) throws IOException;

    TxnNetworkLayer accessTxnNetwork() throws IOException;
}
