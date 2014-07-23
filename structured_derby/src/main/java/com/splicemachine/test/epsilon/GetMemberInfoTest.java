package com.splicemachine.test.epsilon;

import com.splicemachine.test.BaseQueryRunner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Scott Fines
 * Date: 7/21/14
 */
public class GetMemberInfoTest extends BaseQueryRunner {
    private static final String fullQueryBaseSql = "SELECT p.id_member, p.num_point, p.id_point, p.id_reward, \n" +
            "p.bonus_code, p.expire_date, m.service_dte, m.end_dte, \n" +
            "m.awards, m.preferences, m.Name, m.address1, m.address2, \n" +
            "m.city, m.state, m.pin_no, m.purge_date, r.description \n" +
            "FROM --SPLICE-PROPERTIES joinOrder=FIXED \n" +
            "EPS.member_info m \n" +
            "INNER JOIN EPS.points p --SPLICE-PROPERTIES index=ie_point3 \n" +
            "ON m.id_member = p.id_member \n";

    private static final String rewardJoinBaseSql = "SELECT p.id_member, p.num_point, p.id_point, p.id_reward, \n" +
            "p.bonus_code, p.expire_date, m.service_dte, m.end_dte, \n" +
            "r.description \n" +
            "FROM --SPLICE-PROPERTIES joinOrder=FIXED \n" +
            "EPS.points p --SPLICE-PROPERTIES index=ie_point3 \n";

    private static final String oneJoinBase =
            "SELECT p.id_member, p.num_point, p.id_point, p.id_reward, \n" +
            "p.bonus_code, p.expire_date, m.service_dte, m.end_dte, \n" +
            "m.awards, m.preferences, m.Name, m.address1, m.address2, \n" +
            "m.city, m.state, m.pin_no, m.purge_date\n" +
            "FROM --SPLICE-PROPERTIES joinOrder=FIXED \n" +
            "EPS.member_info m \n" +
            "INNER JOIN EPS.points p --SPLICE-PROPERTIES index=ie_point3 \n" +
            "ON m.id_member = p.id_member \n";

    private static final String memberScanBaseSql =
            "SELECT m.service_dte, m.end_dte, \n" +
                    "m.awards, m.preferences, m.Name, m.address1, m.address2, \n" +
                    "m.city, m.state, m.pin_no, m.purge_date\n" +
                    "FROM --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                    "EPS.member_info m \n";

    private static final String pointsScanBaseSql =
            "SELECT p.id_member, p.num_point, p.id_point, p.id_reward, \n" +
                    "p.bonus_code, p.expire_date\n" +
                    "FROM --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                    "EPS.points p --SPLICE-PROPERTIES index=ie_point3 \n";

    private String sql;
    @Override
    protected Runner newRunner(Connection jdbcConn,
                               int numIterations,
                               File outputDir,
                               int threadId,
                               CountDownLatch startLatch) throws IOException {
        final Random random = new Random(System.nanoTime());
        return new Runner(jdbcConn,
                numIterations,
                outputDir,
                threadId,startLatch) {
            @Override
            protected void executeIteration(int iteration, PreparedStatement ps) throws SQLException, IOException {
                ps.setInt(1,random.nextInt(10000000)+1);
                ResultSet rs = ps.executeQuery();
                long size = 0;
                while(rs.next()){
                    size++;
                }
                reportSize(iteration,size);
            }

            @Override
            protected PreparedStatement getPreparedStatement() throws Exception {
                return connection.prepareStatement(sql);
            }
        };
    }

    @Override
    protected void parseAdditionalOptions(CommandLine cli) throws Exception {
        String choice = cli.getOptionValue("q","full");
        if(choice.equals("scan")){
            sql = getScanSql(cli);
        }else if(choice.equals("join"))
            sql = getJoinSql(cli);
        else if(choice.equals("bjoin"))
            sql = getBJoinSql(cli);
        else
            sql = getFullSql(cli);
    }

    private String getBJoinSql(CommandLine cli){
        String joinStrategy = cli.getOptionValue("j","BROADCAST");

        String str;
        if ("none".equalsIgnoreCase(joinStrategy)) {
            str = rewardJoinBaseSql + "INNER JOIN EPS.rewards r \n";
        } else {
            str = rewardJoinBaseSql + "INNER JOIN EPS.rewards r --SPLICE-PROPERTIES joinStrategy=" + joinStrategy + " \n";
        }
        str+= "ON p.id_reward=r.id_reward \n" +
                "WHERE m.id_member=? ";

        return str;
    }

    private String getJoinSql(CommandLine cli) {
        return oneJoinBase+"WHERE m.id_member=?";
    }

    private String getScanSql(CommandLine cli) {
        String table = cli.getOptionValue("qs","member_info");
        if(table.equals("member_info"))
            return memberScanBaseSql+" WHERE m.id_member = ?";
        else return pointsScanBaseSql+" WHERE p.id_member = ?";
    }

    private String getFullSql(CommandLine cli) {
        String joinStrategy = cli.getOptionValue("j","BROADCAST");

        String str;
        if ("none".equalsIgnoreCase(joinStrategy)) {
            str = fullQueryBaseSql + "INNER JOIN EPS.rewards r \n";
        } else {
            str = fullQueryBaseSql + "INNER JOIN EPS.rewards r --SPLICE-PROPERTIES joinStrategy=" + joinStrategy + " \n";
        }
        str+= "ON p.id_reward=r.id_reward \n" +
                "WHERE m.id_member=? ";
        return str;
    }

    @Override
    protected Options getOptions() {
        Options o = super.getOptions();
        Option j = new Option("j", "joinStrategy", true, "A Join strategy to use when joining the rewards table");
        j.setRequired(false);
        o.addOption(j);

        Option queryChoice = new Option("q","query",true,"Which Query to use. Can be either scan,join, or full.Default is full");
        queryChoice.setRequired(false);
        o.addOption(queryChoice);

        Option scanTableChoice = new Option("qs","scanTable",true,"Which Table to scan. can be points or member_info. only applies when query is scan");
        scanTableChoice.setRequired(false);
        o.addOption(scanTableChoice);

        return o;
    }

    public static void main(String...args) throws Exception{
        new GetMemberInfoTest().run(args);
    }
}
