package com.splicemachine.derby.ddl;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import java.util.Set;

public interface DDLWatcher {
    public void registerLanguageConnectionContext(LanguageConnectionContext lcc);
    public void start() throws StandardException;
    public Set<DDLChange> getTentativeDDLs();
}
