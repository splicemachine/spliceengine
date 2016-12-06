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
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by jfilali on 12/6/16.
 */
public class ProcedureUtils {
    public static ResultSet generateResult(String outcome, String message) throws StandardException, SQLException {
        if (message == null) {
            message = "";
        }
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();;

        DataTypeDescriptor dtd =
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, message.length());
        ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{new GenericColumnDescriptor(outcome, dtd)};
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
