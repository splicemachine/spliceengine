package com.splicemachine.derby.impl.sql.execute.operations;

import org.junit.Assert;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class HasherTest {
    DataValueFactory dvf = new J2SEDataValueFactory();

    @Test
    public void testHashSortsTwoPrefixKeys() throws Exception{
        DataValueDescriptor[] t1 = new DataValueDescriptor[2];
        t1[0] = dvf.getVarcharDataValue("jzhang");
        t1[1] = new SQLInteger(1);

        DataValueDescriptor[] t2 = new DataValueDescriptor[2];
        t2[0] = dvf.getVarcharDataValue("sfines");
        t2[1] = new SQLInteger(2);

        Hasher h1 = new Hasher(t1,new int[]{0,1},null,dvf.getVarcharDataValue("prefix1"));
        Hasher h2 = new Hasher(t2,new int[]{0,1},null,dvf.getVarcharDataValue("prefix2"));

        byte[] b1 = h1.generateSortedHashKey(t1);
        byte[] b2 = h2.generateSortedHashKey(t2);

        Assert.assertTrue(Bytes.compareTo(b1,b2)<0);
    }
}
