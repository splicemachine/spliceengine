package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;

/**
 * Created by dgomezferro on 11/24/15.
 */
public interface DDLAction {
    void accept(DDLMessage.DDLChange message);
}
