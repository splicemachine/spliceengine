package com.splicemachine.nist;

import com.splicemachine.derby.nist.DerbyEmbedConnection;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.util.List;

public class DerbyNistTest extends BaseNistTest {
	protected static Connection connection;

    static {
		System.setProperty("derby.system.home", getBaseDirectory()+"/target/derby");
	}
	
	public static void setup() throws Exception {
        File derbyDir = new File(getBaseDirectory()+"/target/derby");
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File nistDir = new File(getBaseDirectory()+"/target/nist/");
        if (nistDir.exists())
        	FileUtils.deleteDirectory(nistDir);
		connection = DerbyEmbedConnection.getCreateConnection();
		connection.close();
    }

    public static void runDerby(List<File> filesToTest) throws Exception {

        connection = DerbyEmbedConnection.getConnection();
        connection.setAutoCommit(false);
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