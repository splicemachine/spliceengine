package com.splicemachine.hbase.backup;


import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.lang.reflect.Method;

/**
 * Created by jyuan on 3/20/15.
 */
@Ignore
public class RestoreTest {

    @Test
    public void testRegionFallsInto() throws StandardException{

        try {
            Restore restore = new Restore();
            Class klass = restore.getClass();
            Class[] params = {byte[].class, byte[].class, byte[].class, byte[].class};
            Method method = klass.getDeclaredMethod("regionFallsInto", params);
            method.setAccessible(true);

            byte[] startKey1 = new byte[0];
            byte[] endKey1 = new byte[0];
            byte[] startKey2 = new byte[0];
            byte[] endKey2 = new byte[0];
            Assert.assertTrue((boolean) method.invoke(restore, startKey1, endKey1, startKey2, endKey2));

            startKey1 = new byte[1];
            startKey1[0] = 10;
            endKey1 = new byte[0];
            startKey2 = new byte[0];
            endKey2 = new byte[0];
            Assert.assertTrue((boolean) method.invoke(restore, startKey1, endKey1, startKey2, endKey2));
            Assert.assertFalse((boolean) method.invoke(restore, startKey2, endKey2, startKey1, endKey1));

            startKey1 = new byte[0];
            endKey1 = new byte[1];
            endKey1[0] = 20;
            startKey2 = new byte[0];
            endKey2 = new byte[0];
            Assert.assertTrue((boolean) method.invoke(restore, startKey1, endKey1, startKey2, endKey2));
            Assert.assertFalse((boolean) method.invoke(restore, startKey2, endKey2, startKey1, endKey1));

            startKey1 = new byte[1]; startKey1[0] = 10;
            endKey1 = new byte[1]; endKey1[0] = 20;
            startKey2 = new byte[1]; startKey2[0] = 30;
            endKey2 = new byte[1]; endKey2[0] = 40;
            Assert.assertFalse((boolean) method.invoke(restore, startKey1, endKey1, startKey2, endKey2));
            Assert.assertFalse((boolean) method.invoke(restore, startKey2, endKey2, startKey1, endKey1));

            startKey1 = new byte[1]; startKey1[0] = 10;
            endKey1 = new byte[1]; endKey1[0] = 30;
            startKey2 = new byte[1]; startKey2[0] = 20;
            endKey2 = new byte[1]; endKey2[0] = 40;
            Assert.assertFalse((boolean) method.invoke(restore, startKey1, endKey1, startKey2, endKey2));
            Assert.assertFalse((boolean) method.invoke(restore, startKey2, endKey2, startKey1, endKey1));

            startKey1 = new byte[1]; startKey1[0] = 10;
            endKey1 = new byte[1]; endKey1[0] = 20;
            startKey2 = new byte[1]; startKey2[0] = 0;
            endKey2 = new byte[1]; endKey2[0] = 40;
            Assert.assertTrue((boolean) method.invoke(restore, startKey1, endKey1, startKey2, endKey2));
            Assert.assertFalse((boolean) method.invoke(restore, startKey2, endKey2, startKey1, endKey1));
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }
}
