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
