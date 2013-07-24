package com.splicemachine.nist;

import com.splicemachine.derby.nist.ConnectionPool;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.util.List;

public class DerbyNistTest extends BaseNistTest {

    static {
		System.setProperty("derby.system.home", getBaseDirectory()+"/target/derby");
	}
    private final ConnectionPool pool;

    public DerbyNistTest(ConnectionPool pool) throws Exception {
        File derbyDir = new File(getBaseDirectory()+"/target/derby");
        if (derbyDir.exists())
        	FileUtils.deleteDirectory(derbyDir);
        File nistDir = new File(getBaseDirectory()+"/target/nist/");
        if (nistDir.exists())
        	FileUtils.deleteDirectory(nistDir);

        this.pool = pool;
    }

    public void runDerby(List<File> filesToTest) throws Exception {

        for (File file: filesToTest) {
            Connection connection = pool.getConnection();
            connection.setAutoCommit(false);
            runTest(file, DERBY_OUTPUT_EXT, connection);
//            connection.commit();
            pool.returnConnection(connection);
        }
//        try {
//            connection.close();
//        } catch (Exception e) {
//            connection.commit();
//            connection.close();
//        }
    }

}