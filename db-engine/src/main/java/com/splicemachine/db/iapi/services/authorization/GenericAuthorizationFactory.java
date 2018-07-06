package com.splicemachine.db.iapi.services.authorization;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.sql.conn.GenericAuthorizer;

/**
 * Created by jleach on 3/19/18.
 */
public class GenericAuthorizationFactory implements AuthorizationFactory {
    @Override
    public Authorizer getAuthorizer(LanguageConnectionContext lcc) throws StandardException {
        return new GenericAuthorizer(lcc);
    }

    @Override
    public int getPriority() {
        return 1;
    }
}
