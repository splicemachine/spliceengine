package com.splicemachine.nist;

import com.splicemachine.derby.nist.ConnectionFactory;
import com.splicemachine.derby.nist.ConnectionPool;
import com.splicemachine.derby.nist.DerbyEmbedConnection;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.util.List;

public class DerbyNistTest extends BaseNistTest {
    private static final Logger LOG = Logger.getLogger(DerbyNistTest.class);

    static {
		System.setProperty("derby.system.home", getBaseDirectory()+"/target/derby");
	}

    public DerbyNistTest() throws Exception {
        File derbyDir = new File(getBaseDirectory()+"/target/derby");
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File nistDir = new File(getBaseDirectory()+"/target/nist/");
        if (nistDir.exists())
        	FileUtils.deleteDirectory(nistDir);
        Connection connection = DerbyEmbedConnection.getConnection();
        connection.close();
    }

    public void runDerby(List<File> filesToTest) throws Exception {

        Connection connection = DerbyEmbedConnection.getConnection();
        connection.setAutoCommit(true);
        for (File file: filesToTest) {
           runTest(file, DERBY_OUTPUT_EXT, connection);
        }
        try {
            connection.close();
        } catch (Exception e) {
            connection.commit();
            connection.close();
        }
    }

}