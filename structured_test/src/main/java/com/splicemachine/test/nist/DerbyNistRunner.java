package com.splicemachine.test.nist;

import com.splicemachine.test.connection.DerbyEmbedConnection;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.util.List;

public class DerbyNistRunner extends NistTestUtils {
    private static final Logger LOG = Logger.getLogger(DerbyNistRunner.class);

    static {
		System.setProperty("derby.system.home", NistTestUtils.getBaseDirectory()+"/target/derby");
	}

    public DerbyNistRunner() throws Exception {
        File derbyDir = new File(NistTestUtils.getBaseDirectory()+"/target/derby");
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File nistDir = new File(NistTestUtils.getBaseDirectory()+"/target/nist/");
        if (nistDir.exists())
        	FileUtils.deleteDirectory(nistDir);
        Connection connection = DerbyEmbedConnection.getConnection();
        connection.close();
    }

    public void runDerby(List<File> filesToTest) throws Exception {

        Connection connection = DerbyEmbedConnection.getConnection();
        for (File file: filesToTest) {
            NistTestUtils.runTest(file, NistTestUtils.DERBY_OUTPUT_EXT, connection);
        }
        try {
            connection.close();
        } catch (Exception e) {
            connection.commit();
            connection.close();
        }
    }

}