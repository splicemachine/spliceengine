package com.splicemachine.derby.utils;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldEncoder;
import net.sf.ehcache.config.PinningConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.J2SEDataValueFactory;
import com.splicemachine.db.iapi.types.StringDataValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

import com.splicemachine.db.iapi.types.SQLDate;
/**
* Created by yifu on 6/24/14.
*/
public class setFromTest {
    protected static J2SEDataValueFactory dvf = new J2SEDataValueFactory();

    @BeforeClass
    public static void startup() throws StandardException {
        ModuleFactory monitor = Monitor.getMonitorLite();
        Monitor.setMonitor(monitor);
        monitor.setLocale(new Properties(), Locale.getDefault().toString());
        dvf.boot(false, new Properties());
    }
    /*
    As the setFrom method is set as protected in derby, we here use the setValue public method which is what setFrom
     really invokes in its execution
     */
    @Test
    public void testSetFromMethod()throws Exception{
        Timestamp ts;
        DataValueDescriptor des;
        SQLDate test;
        for(int i=0;i<100000;i++){
            test = new SQLDate();
            Calendar c = Calendar.getInstance();
                c.add(Calendar.DAY_OF_YEAR,i);
                ts = new Timestamp(c.getTime().getTime());
                des = dvf.getNull(StoredFormatIds.SQL_DATE_ID, StringDataValue.COLLATION_TYPE_UCS_BASIC);
                des.setValue(ts);
                if(des!=null) {
                    test.setValue(des);
                }
            /*
            Assert timestamp and the SQLdate equals, proving that setValue method always return a value
             */
            Assert.assertNotNull(test);
          Assert.assertEquals((new Date(ts.getTime())).toString(),test.toString());
        }

    }


}
