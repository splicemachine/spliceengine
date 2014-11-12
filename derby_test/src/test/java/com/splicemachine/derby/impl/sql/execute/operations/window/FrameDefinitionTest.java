package com.splicemachine.derby.impl.sql.execute.operations.window;

import org.junit.Test;

/**
 * @author Jeff Cunningham
 *         Date: 9/24/14
 */
public class FrameDefinitionTest {

    @Test
    public void frameDefnToString() throws Exception {
        // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        System.out.println(WindowTestingFramework.DEFAULT_FRAME_DEF);
        // ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        System.out.println(WindowTestingFramework.ONE_PREC_CUR_ROW_FRAME_DEF);
        // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        System.out.println(WindowTestingFramework.ALL_ROWS);
        // ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        System.out.println(new FrameDefinition(FrameDefinition.FrameMode.ROWS.ordinal(),
                                               FrameDefinition.Frame.PRECEDING.ordinal(), 2,
                                               FrameDefinition.Frame.CURRENT_ROW.ordinal(), -1));
        // ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
        System.out.println(new FrameDefinition(FrameDefinition.FrameMode.ROWS.ordinal(),
                                               FrameDefinition.Frame.PRECEDING.ordinal(), 10,
                                               FrameDefinition.Frame.FOLLOWING.ordinal(), 10));
        // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        System.out.println(new FrameDefinition(FrameDefinition.FrameMode.RANGE.ordinal(),
                                               FrameDefinition.Frame.UNBOUNDED_PRECEDING.ordinal(), -1,
                                               FrameDefinition.Frame.UNBOUNDED_FOLLOWING.ordinal(), -1));
    }

}
