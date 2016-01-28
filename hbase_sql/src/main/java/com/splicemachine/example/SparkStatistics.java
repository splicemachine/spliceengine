package com.splicemachine.example;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import java.sql.*;

public class SparkStatistics {
    public static void getStatementStatistics(String statement) throws SQLException {
        try {
            Connection con = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement ps = con.prepareStatement(statement);
            ResultSet rs = ps.executeQuery();
            JavaRDD<LocatedRow> resultSetRDD = SparkMLibUtils.resultSetToRDD(rs);
            JavaRDD<Vector> vectorJavaRDD = SparkMLibUtils.locatedRowRDDToVectorRDD(resultSetRDD,new int[]{1,2});
            MultivariateStatisticalSummary summary = Statistics.colStats(vectorJavaRDD.rdd());
       } catch (StandardException e) {
            throw new SQLException("spark error");
        }
    }
}
