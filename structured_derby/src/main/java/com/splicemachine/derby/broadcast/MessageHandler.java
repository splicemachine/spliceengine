package com.splicemachine.derby.broadcast;

import com.splicemachine.derby.ddl.DDLChange;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import java.util.Set;

public interface MessageHandler {
    public void handleMessage(byte[] message) throws StandardException;
}
