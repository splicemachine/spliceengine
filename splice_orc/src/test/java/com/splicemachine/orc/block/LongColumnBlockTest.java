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
package com.splicemachine.orc.block;

import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 */
public class LongColumnBlockTest {

    @Test
    public void setPartitionValueTest() {
        LongColumnBlock longColumnBlock = new LongColumnBlock(null, DataTypes.LongType);
        longColumnBlock.setPartitionValue("45",1000);
        for (int i = 0; i< 1000; i++) {
            Assert.assertEquals(45l,longColumnBlock.getTestObject(i));
        }
    }

}
