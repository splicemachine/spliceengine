package org.apache.derby.impl.sql.execute.operations.joins;

import java.sql.PreparedStatement;

import org.junit.Ignore;

import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.derby.test.SpliceDerbyTest;

@Ignore
public class BaseJoinTest extends SpliceDerbyTest{

	public static void createData(DerbyTestRule rule) throws Exception {
		rule.setAutoCommit(true);
		PreparedStatement psC = rule.prepareStatement("insert into c values (?,?)");
		PreparedStatement psD = rule.prepareStatement("insert into d values (?,?)");
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
		rule.commit();
	}
}
