/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;

public class MonGetConnectionIT {
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void selectFromMonGetConnectionWorks() throws Exception{
        try(ResultSet rs = methodWatcher.executeQuery("SELECT * FROM TABLE(SYSIBMADM.MON_GET_CONNECTION()) X")) {
            // should be empty
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void selectFromSYSIBMADMApplicationsWorks() throws Exception{
        try(ResultSet rs = methodWatcher.executeQuery("SELECT * FROM SYSIBMADM.APPLICATIONS")) {
            // should be empty
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void selectFromSYSIBMADMSNAPAPPL_INFOWorks() throws Exception{
        try(ResultSet rs = methodWatcher.executeQuery("SELECT * FROM SYSIBMADM.SNAPAPPL_INFO")) {
            // should be empty
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void selectFromSYSIBMADMSNAPAPPLWorks() throws Exception{
        try(ResultSet rs = methodWatcher.executeQuery("SELECT * FROM SYSIBMADM.SNAPAPPL")) {
            // should be empty
            Assert.assertFalse(rs.next());
        }
    }
}
