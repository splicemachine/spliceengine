/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
