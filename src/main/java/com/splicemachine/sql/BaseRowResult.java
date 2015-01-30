package com.splicemachine.sql;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 1/28/15
 */
public abstract class BaseRowResult implements RowResult{

    /*****************************************************************************************************************/
    /*Unsupported by default methods*/
    @Override public InputStream getAsciiStream(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public InputStream getUnicodeStream(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public InputStream getBinaryStream(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public Reader getNCharacterStream(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public Clob getClob(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public Blob getBlob(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public Array getArray(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public Reader getCharacterStream(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public SQLXML getSQLXML(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public NClob getNClob(int columnIndex) { throw new UnsupportedOperationException(); }
    @Override public Ref getRef(int columnIndex) { throw new UnsupportedOperationException(); }
}
