package com.splicemachine.derby.impl.sql;

import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class LocalOlapClient implements OlapClient{
    private static LocalOlapClient ourInstance=new LocalOlapClient();

    public static LocalOlapClient getInstance(){
        return ourInstance;
    }

    private LocalOlapClient(){ }

    @Override
    public <R extends OlapResult> R submitOlapJob(OlapJobRequest jobRequest) throws IOException {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
