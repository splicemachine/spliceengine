/*
 * Copyright (c) 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLDecimal;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

@Category(ArchitectureIndependent.class)
public class DecimalDescriptorSerializerTest {

    @Test
    public void testDecodeDirect_MultipleValues() throws StandardException {
        DescriptorSerializer dds = DecimalDescriptorSerializer.INSTANCE_FACTORY.newInstance();
        testDecodeDirect( dds, "10.000000", 8, 6);
        testDecodeDirect( dds, "10.000001", 8, 6);
        testDecodeDirect( dds, "1.000000", 7, 6);
        testDecodeDirect( dds, "1.000001", 7, 6);
        testDecodeDirect( dds, "0.100000", 6, 6);
        testDecodeDirect( dds, "0.100001", 6, 6);
        testDecodeDirect( dds, "0.010000", 6, 6);
        testDecodeDirect( dds, "0.010001", 6, 6);
        testDecodeDirect( dds, "0.001000", 6, 6);
        testDecodeDirect( dds, "0.001001", 6, 6);
        testDecodeDirect( dds, "1", 1, 0);
        testDecodeDirect( dds, "1.0", 2, 1);
        testDecodeDirect( dds, "1.1", 2, 1);
        testDecodeDirect( dds, "1.00", 3, 2);
        testDecodeDirect( dds, "1.01", 3, 2);
        testDecodeDirect( dds, "1.10", 3, 2);
        testDecodeDirect( dds, "1.010", 4, 3);
    }
    
    public void testDecodeDirect(DescriptorSerializer dds, String value, int precision, int scale) throws StandardException {
        BigDecimal bdSource = new BigDecimal(value);
        SQLDecimal source = new SQLDecimal( bdSource );
        byte[] bytes = dds.encodeDirect(source, false);
        
        SQLDecimal target = new SQLDecimal(null, precision, scale);
        dds.decodeDirect(
            target,
            bytes,
            0, bytes.length, false
        );

        assertEquals(
            stringOf(new SQLDecimal( bdSource.stripTrailingZeros() ), precision, scale),
            stringOf(target, target.getPrecision(), target.getScale())
        );
    }

    private String stringOf(SQLDecimal dec, int precision, int scale) {
        return dec+","+precision+","+scale;
    }
}
