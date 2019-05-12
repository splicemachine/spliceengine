package com.splicemachine.db.impl.drda;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DRDAProtocolExceptionTest {

    @Test
    public void typoTest() {

        DRDAProtocolException exc;

        String msgid = "DRDA_Disconnect"; //errorInfoTable.get
        DRDAConnThread agent = mock(DRDAConnThread.class); // mock_it framework

        when(agent.getCorrelationID()).thenReturn(0);
        when(agent.getCrrtkn()).thenReturn(null);

        int cpArg = CodePoint.PRDID;
        int errCdArg = 0;
        Object[] args = null;

        exc = new DRDAProtocolException(msgid, agent, cpArg, errCdArg, args);

        String result = exc.getMessage();

        Assert.assertEquals(result,"Execution failed because of invalid client connection attempt, " +
                                          "please use Splice Machine Driver.");
    }
}
