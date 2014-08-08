package com.splicemachine.derby.security;

import java.sql.SQLException;

import java.sql.SQLSyntaxErrorException;

import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;


import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceRoleWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;


//@Ignore()
public class DerbyAuthorizationAndAuthenticationIT extends SpliceUnitTest { 
    private static final String SCHEMA_NAME = DerbyAuthorizationAndAuthenticationIT.class.getSimpleName().toUpperCase();
    protected static final String USER1 = "john";
    protected static final String PASSWORD1 = "jleach";
    protected static final String USER2 = "jim";
    protected static final String PASSWORD2 = "bo";
    protected static final String USER3 = "suzy";
    protected static final String PASSWORD3 = "X)X)X"; // Do passwords have to be escaped, etc. ? (JL)
    protected static final String INSERT_STATEMENT = "insert into %s values ('1','Johnny',1.2,'Detroit')";
    protected static final String INSERT_STATEMENT1Rev2 = "insert into %s values ('2','Johnny',1.2,'Detroit')";
    protected static final String UPDATE_STATEMENT1Rev2 = "update %s set CITY = 'STL' where EMPNUM = '2'"; 
    protected static final String ROLE1 = "super_user";
    protected static final String ROLE2 = "read_only";
    protected static final String ROLE3 = "app_user";
    
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA_NAME+USER1,USER1);
    protected static SpliceSchemaWatcher spliceSchemaWatcher2 = new SpliceSchemaWatcher(SCHEMA_NAME+USER2,USER2);
    protected static SpliceSchemaWatcher spliceSchemaWatcher3 = new SpliceSchemaWatcher(SCHEMA_NAME+USER3,USER3);
    protected static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1,PASSWORD1);
    protected static SpliceUserWatcher spliceUserWatcher2 = new SpliceUserWatcher(USER2,PASSWORD2);
    protected static SpliceUserWatcher spliceUserWatcher3 = new SpliceUserWatcher(USER3,PASSWORD3);
    protected static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    protected static SpliceRoleWatcher spliceRoleWatcher2 = new SpliceRoleWatcher(ROLE2);
    protected static SpliceRoleWatcher spliceRoleWatcher3 = new SpliceRoleWatcher(ROLE3);
    
    protected static SpliceTableWatcher staffTableUser1 = new SpliceTableWatcher("STAFF",spliceSchemaWatcher1.schemaName,
            "(EMPNUM   VARCHAR(3) NOT NULL, "+
            "EMPNAME  VARCHAR(20), "+
            "GRADE    DECIMAL(4), "+
            "CITY     VARCHAR(15))", USER1, PASSWORD1);
    protected static SpliceTableWatcher staffTableUser2 = new SpliceTableWatcher("STAFF",spliceSchemaWatcher2.schemaName,
            "(EMPNUM   VARCHAR(3) NOT NULL, "+
            "EMPNAME  VARCHAR(20), "+
            "GRADE    DECIMAL(4), "+
            "CITY     VARCHAR(15))", USER2, PASSWORD2);
    protected static SpliceTableWatcher staffTableUser3 = new SpliceTableWatcher("STAFF",spliceSchemaWatcher3.schemaName,
            "(EMPNUM   VARCHAR(3) NOT NULL, "+
            "EMPNAME  VARCHAR(20), "+
            "GRADE    DECIMAL(4), "+
            "CITY     VARCHAR(15))", USER3, PASSWORD3);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceUserWatcher1)
            .around(spliceUserWatcher2)
            .around(spliceUserWatcher3)
            .around(spliceSchemaWatcher1)
            .around(spliceSchemaWatcher2)
            .around(spliceSchemaWatcher3)            
            .around(staffTableUser1)
            .around(staffTableUser2)
            .around(staffTableUser3)
            .around(spliceRoleWatcher1)
            .around(spliceRoleWatcher2)
            .around(spliceRoleWatcher3);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();
    
    @Test(expected=IllegalStateException.class)
    public void validateBadAuthentication() {
    	SpliceNetConnection.getConnectionAs(USER1, "dssfdsdf");
    }

    @Test()
    public void validateSuccessfulAuthentication() {
    	SpliceNetConnection.getConnectionAs(USER1, PASSWORD1);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotSeePasswordsInSysUsers() throws Exception {
    	methodWatcher.executeQuery("select * from sys.sysusers", USER1, PASSWORD1);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testSuperUserCannotSeePasswordsInSysUsers() throws Exception {
    	methodWatcher.executeQuery("select * from sys.sysusers");
    }
    
    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotSelectFromAnotherUsersTable() throws Exception {
    	methodWatcher.executeQuery(format("select * from %s",staffTableUser1.toString()),USER2,PASSWORD2);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotAggregateFromAnotherUsersTable() throws Exception {
    	methodWatcher.executeQuery(format("select count(*) from %s",staffTableUser1.toString()),USER2,PASSWORD2);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotInsertIntoAnotherUsersTable() throws Exception {
    	methodWatcher.executeUpdate(format(INSERT_STATEMENT,staffTableUser1.toString()),USER2,PASSWORD2);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotUpdateAnotherUsersTable() throws Exception {
    	methodWatcher.executeUpdate(format(INSERT_STATEMENT1Rev2,staffTableUser1.toString()),USER1,PASSWORD1);
    	methodWatcher.executeUpdate(format(UPDATE_STATEMENT1Rev2,staffTableUser1.toString()),USER2,PASSWORD2);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotDropAnotherUsersTable() throws Exception {
    	methodWatcher.executeUpdate(format("drop table %s",staffTableUser1.toString()),USER2,PASSWORD2);
    }

    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotAddTableInSomeoneElsesSchema() throws Exception {
    	methodWatcher.executeUpdate(format("create table %s.foo (col1 int)",spliceSchemaWatcher1.schemaName),USER2,PASSWORD2);
    }
    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotAddIndexInSomeoneElsesSchema() throws Exception {
    	methodWatcher.executeUpdate(format("create index %s.foo on %s (EMPNUM)",spliceSchemaWatcher1.schemaName,staffTableUser1),USER2,PASSWORD2);
    }
    @Test(expected=SQLSyntaxErrorException.class)
    public void testUserCannotAddViewInSomeoneElsesSchema() throws Exception {
    	methodWatcher.executeUpdate(format("create view %s.foo as select * from %s",spliceSchemaWatcher1.schemaName,staffTableUser1),USER2,PASSWORD2);
    }

    @Test(expected=SQLException.class)
    public void testCannotCreateRoleWithUserName() throws Exception {
    	methodWatcher.executeUpdate(format("create role %s",USER2));
    }

    @Test()
    public void testGrantSelectAndRevoke() throws Exception {
    	methodWatcher.executeUpdate(format("grant select on table %s to %s",staffTableUser1,USER2));
    	methodWatcher.executeUpdate(format("grant select (empname,city) on table %s to %s",staffTableUser1,USER3));
    	methodWatcher.executeQuery(format("select * from %s", staffTableUser1), USER2,PASSWORD2);
    	methodWatcher.executeQuery(format("select empname, city from %s", staffTableUser1), USER3,PASSWORD3);
    	try {
    		methodWatcher.executeQuery(format("select * from %s", staffTableUser1), USER3,PASSWORD3);
    		Assert.assertTrue("Column List was greater than authorized columns",false);
    	} catch (SQLException e) {} // Swallow
    	methodWatcher.executeUpdate(format("revoke select on table %s from %s",staffTableUser1,USER2),SpliceNetConnection.DEFAULT_USER,SpliceNetConnection.DEFAULT_USER_PASSWORD);
    	methodWatcher.executeUpdate(format("revoke select (empname,city) on table %s from %s",staffTableUser1,USER3),SpliceNetConnection.DEFAULT_USER,SpliceNetConnection.DEFAULT_USER_PASSWORD);
    }

}
