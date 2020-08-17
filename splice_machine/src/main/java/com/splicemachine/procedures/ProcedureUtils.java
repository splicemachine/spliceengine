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

package com.splicemachine.procedures;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.SpliceAdmin;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by jfilali on 12/6/16.
 */
public class ProcedureUtils {
     static public ResultSet generateResult(String outcome, String message) throws StandardException, SQLException {
        if (message == null) {
            message = "";
        }
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

        DataTypeDescriptor dtd =
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, message.length());
        ResultColumnDescriptor[] rcds = {new GenericColumnDescriptor(outcome, dtd)};
        ExecRow template = new ValueRow(1);
        template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
        List<ExecRow> rows = Lists.newArrayList();
        template.getColumn(1).setValue(message);

        rows.add(template.getClone());
        IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
        inprs.openCore();
        return new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
    }
}
