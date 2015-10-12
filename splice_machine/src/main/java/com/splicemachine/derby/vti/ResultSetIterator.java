package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Iterator;

/**
 *
 */
@NotThreadSafe
public class ResultSetIterator implements Iterable<LocatedRow>, Iterator<LocatedRow>, Closeable {
    private ExecRow execRow;
    private ResultSet resultSet;
    private boolean[] isNullable;
    private Connection connection;
    private PreparedStatement ps;


    public ResultSetIterator(Connection connection, PreparedStatement ps, ExecRow execRow) {
        this.connection = connection;
        this.ps = ps;
        try {
            this.resultSet = ps.executeQuery();
            ResultSetMetaData rsm = resultSet.getMetaData();
            isNullable = new boolean[rsm.getColumnCount()];
            for (int i =0;i<rsm.getColumnCount();i++) {
                isNullable[i] = rsm.isNullable(i+1) ==0?false:true;
            }
        }  catch (Exception e) {
            try {
                if (connection != null)
                    connection.close();
            } catch (Exception cE) {
                throw new RuntimeException(cE);
            }
            throw new RuntimeException(e);
        }
        this.execRow = execRow;
    }

    @Override
    public Iterator<LocatedRow> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LocatedRow next() {
        try {
            ExecRow returnRow = execRow.getClone();
            for (int i = 1; i <= execRow.nColumns(); i++) {
                returnRow.getColumn(i).setValueFromResultSet(resultSet,i,isNullable[i-1]);
            }
            return new LocatedRow(returnRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception se) {
                throw new IOException(se);
            }
        }
    }
}
