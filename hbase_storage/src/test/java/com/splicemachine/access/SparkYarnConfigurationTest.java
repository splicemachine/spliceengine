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

package com.splicemachine.access;

import static org.junit.Assert.*;
import org.junit.Test;
import java.io.*;


/**
 * Test SparkYarnConfigurationTest.
 */
public class SparkYarnConfigurationTest {
    SparkYarnConfiguration conf = new SparkYarnConfiguration();

    @Test
    public void testSerDe() throws Exception {
        conf.setNumNodes(1);
        conf.setYarnExecutorMemoryOverhead("2");
        conf.setExecutorMemoryOverhead("3");
        conf.setExecutorMemory("4");
        conf.setYarnExecutorMemoryOverhead(null);
        conf.setDynamicAllocationMaxExecutors("6");
        conf.setExecutorCores("7");
        conf.setExecutorInstances("8");
        conf.setYarnNodemanagerResourceMemoryMB("9");

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(conf);
        oos.flush();
        byte [] serializedConfig = bos.toByteArray();

        SparkYarnConfiguration newConf;
        ByteArrayInputStream bis = new ByteArrayInputStream(serializedConfig);
        ObjectInput in = new ObjectInputStream(bis);
        newConf = (SparkYarnConfiguration) in.readObject();
        assertTrue("SerDe failed!", conf.equals(newConf));
    }
}
