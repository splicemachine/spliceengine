package org.apache.derby.impl.sql.execute.operations.joins;

import java.sql.PreparedStatement;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Ignore;

import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.derby.test.SpliceDerbyTest;


public class BaseJoinTest extends SpliceDerbyTest{

	public static void insertData(String t1,String t2,SpliceWatcher spliceWatcher) throws Exception {
        spliceWatcher.setAutoCommit(true);
		PreparedStatement psC = spliceWatcher.prepareStatement("insert into "+t1+" values (?,?)");
		PreparedStatement psD = spliceWatcher.prepareStatement("insert into "+t2+" values (?,?)");
		for (int i =0; i< 10; i++) {
			psC.setString(1, "" + i);
			psC.setString(2, "i");
			psC.executeUpdate();
			if (i!=9) {
				psD.setString(1, "" + i);
				psD.setString(2, "i");
				psD.executeUpdate();
			}
		}
        spliceWatcher.commit();
	}
}
