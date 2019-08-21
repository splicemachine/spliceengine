package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import org.apache.calcite.plan.Context;

/**
 * Created by yxia on 8/20/19.
 */
public class SpliceContext implements Context {
    LanguageConnectionContext lcc;

    SpliceContext(LanguageConnectionContext lcc) {
        this.lcc = lcc;
    }

    LanguageConnectionContext getLcc() {
        return lcc;
    }
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        return null;
    }
}
