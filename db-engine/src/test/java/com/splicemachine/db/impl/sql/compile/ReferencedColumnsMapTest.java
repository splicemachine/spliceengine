/*
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
package com.splicemachine.db.iapi.types;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.ReferencedColumnsMap;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * Test Class for ReferencedColumnsMap
 *
 */
public class ReferencedColumnsMapTest {

    @Test
    public void testFunctionality() throws Exception {

        ReferencedColumnsMap map1 = new ReferencedColumnsMap();
        ReferencedColumnsMap map2 = new ReferencedColumnsMap();

        assertTrue("Failed getNumReferencedTables test.", map1.getNumReferencedTables() == 0);

        map1.add(1,1).add(1,2).add(1,3);
        map2.add(1,1);
        assertTrue("Failed getNumReferencedTables test.", map1.getNumReferencedTables() == 1);
        assertTrue("Failed intersection test.", map1.intersects(1, map2));
        assertFalse("Failed intersection test.", map1.intersects(2, map2));
        assertFalse("Failed equals test.", map1.equals(map2));

        ReferencedColumnsMap map3 = new ReferencedColumnsMap(map1);
        assertTrue("Failed equals test.", map1.equals(map3));

        map1.add(2,1);
        map2.add(2,2);
        assertFalse("Failed intersection test.", map1.intersects(2, map2));
        map1.add(2,2);
        assertTrue("Failed intersection test.", map1.intersects(1, map2));
        assertTrue("Failed getNumReferencedTables test.", map1.getNumReferencedTables() == 2);

        Set<Integer> testSet = map1.get(1);
        Set<Integer> validateSet = new HashSet<>(3);
        validateSet.add(1); validateSet.add(2); validateSet.add(3);
        assertTrue("Failed equals test.", testSet.equals(validateSet));
        assertTrue("Failed equals test.", testSet.equals(map1.get(1)));
        ReferencedColumnsMap map4 = new ReferencedColumnsMap(map1);
        map1.add(1,2);
        map1.add(1,2);
        assertTrue("Failed equals test.", map1.equals(map4));
        map1.add(1,4);
        assertFalse("Failed equals test.", map1.equals(map4));

        // Should be able to use table number of zero.
        map1.add(0,2);

        try {
            map1.add(-1,2);
            fail("Not supposed to be able to add a negative table number to the map.");
        }
        catch (StandardException e) {
        }
        try {
            map1.add(-100,0);
            fail("Not supposed to be able to add a negative column number to the map.");
        }
        catch (StandardException e) {
        }
    }

}
