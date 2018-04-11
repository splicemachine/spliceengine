package com.splicemachine.db.iapi.services.authorization;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

/**
 *
 *
 */
public interface AuthorizationFactory {

    Authorizer getAuthorizer(LanguageConnectionContext lcc)
            throws StandardException;

    int getPriority();
}
