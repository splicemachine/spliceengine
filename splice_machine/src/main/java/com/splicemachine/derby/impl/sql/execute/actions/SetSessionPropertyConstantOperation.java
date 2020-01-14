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
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import java.util.Properties;

/**
 * Created by yxia on 5/17/18.
 */
public class SetSessionPropertyConstantOperation implements ConstantAction {
    private final Properties sessionProperties;
    /**
     * Make the ConstantAction for a SET SCHEMA statement.
     *
     *  @param sessionProperties	list of session properties
     */
    public SetSessionPropertyConstantOperation(Properties sessionProperties) {
        this.sessionProperties = sessionProperties;
    }

    public	String	toString() {
        return "SET SESSION PROPERTIES: " + sessionProperties.toString();
    }


    /**
     *	This is the guts of the Execution-time logic for SET SCHEMA.
     *
     *	@see ConstantAction#executeConstantAction
     *
     * @exception StandardException        Thrown on failure
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();

        lcc.setSessionProperties(sessionProperties);
    }
}
