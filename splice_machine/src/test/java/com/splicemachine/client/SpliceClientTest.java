/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.client;

import com.splicemachine.db.client.am.SqlException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SpliceClientTest {

    @Test
    public void testParseJDBCPassword() throws SqlException {
        String password = "Abd98*@80EFg";
        String raw = "jdbc:splice://localhost.com:1527/splicedb;user=af29891;password=" + password;
        assertEquals(password, SpliceClient.parseJDBCPassword(raw));

        raw = "jdbc:splice://localhost:1527/splicedb;password=" + password + ";user=af29891";
        assertEquals(password, SpliceClient.parseJDBCPassword(raw));
    }
}
