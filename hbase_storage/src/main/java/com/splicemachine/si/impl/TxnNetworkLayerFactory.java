package com.splicemachine.si.impl;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface TxnNetworkLayerFactory{

    TxnNetworkLayer accessTxnNetwork() throws IOException;
}
