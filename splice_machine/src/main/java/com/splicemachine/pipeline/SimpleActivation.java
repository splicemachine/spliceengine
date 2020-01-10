/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.splicemachine.pipeline;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.dictionary.StatementPermission;
import com.splicemachine.db.impl.sql.GenericPreparedStatement;
import com.splicemachine.db.impl.sql.execute.BaseActivation;

import java.util.List;
import java.util.Vector;

public class SimpleActivation extends BaseActivation {

    public SimpleActivation(List<StatementPermission> statementPermissions, LanguageConnectionContext context) throws StandardException {
        GenericPreparedStatement gps = new GenericPreparedStatement(null);
        gps.setRequiredPermissionsList(statementPermissions);
        preStmt = gps;
        initFromContext(context);
    }
    @Override
    protected int getExecutionCount() {
        return 0;
    }

    @Override
    protected void setExecutionCount(int newValue) {

    }

    @Override
    protected Vector getRowCountCheckVector() {
        return null;
    }

    @Override
    protected void setRowCountCheckVector(Vector newValue) {

    }

    @Override
    protected int getStalePlanCheckInterval() {
        return 0;
    }

    @Override
    protected void setStalePlanCheckInterval(int newValue) {

    }

    @Override
    public void postConstructor() throws StandardException {

    }

    @Override
    public ResultSet execute() throws StandardException {
        return null;
    }

    @Override
    public void materialize() throws StandardException {

    }

    @Override
    public void prepareToInvalidate(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {

    }

    @Override
    public void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException {
        
    }

    @Override
    public Activation getParentActivation() {
        return null;
    }

}
