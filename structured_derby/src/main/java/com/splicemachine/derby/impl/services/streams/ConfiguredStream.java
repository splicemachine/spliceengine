package com.splicemachine.derby.impl.services.streams;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.impl.services.stream.SingleStream;

/**
 * @author Scott Fines
 *         Date: 10/9/14
 */
public class ConfiguredStream extends SingleStream {

    @Override
    protected HeaderPrintWriter makeStream() {
        String errorFileLocation = PropertyUtil.getSystemProperty(Property.ERRORLOG_FILE_PROPERTY);
        if(errorFileLocation==null){
            /*
             * We haven't explicitly set the error file location in our startup value,
             * so we default it to something nice. In particular, we look for the
             * hbase log4j location to determine where the file should go, and we
             * create a "splice.log" which is located in that directory.
             */
            String hbaseLogDir = PropertyUtil.getSystemProperty("hbase.log.dir");
            String logFileName = PropertyUtil.getSystemProperty("splice.log.file","splice.log");

            errorFileLocation = hbaseLogDir+"/"+logFileName;
            System.setProperty(Property.ERRORLOG_FILE_PROPERTY,errorFileLocation);
        }

        return super.makeStream();
    }
}
