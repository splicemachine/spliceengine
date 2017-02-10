/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import java.sql.ResultSet;

/**
 * @author Scott Fines
 *         Created on: 2/23/13
 */
public class MetatablesIT { 
    private static final Logger LOG = Logger.getLogger(MetatablesIT.class);	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testSelectWithOr() throws Exception{
        /*
         * Set Bug #245 for details on the existence of this test.
         */
        String aliasTable = "SYSALIASES";
        String checksTable = "SYSCHECKS";
        ResultSet rs = methodWatcher.executeQuery("select * from sys.systables where tablename = 'SYSALIASES' or tablename = 'SYSCHECKS'");
        int count = 0;
        while(rs.next()){
            count++;
            String tableName = rs.getString(2);
            Assert.assertTrue("incorrect table returned!",aliasTable.equalsIgnoreCase(tableName)||checksTable.equalsIgnoreCase(tableName));
            LOG.info(String.format("table=%s",tableName));
        }
        Assert.assertEquals("Incorrect count!",2,count);
    }
}
