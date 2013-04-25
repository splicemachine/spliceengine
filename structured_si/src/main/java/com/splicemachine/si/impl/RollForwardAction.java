package com.splicemachine.si.impl;

import java.io.IOException;
import java.util.List;

public interface RollForwardAction {
    void rollForward(long transactionId, List rowList) throws IOException;
}
