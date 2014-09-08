package com.splicemachine.test_dao;

import com.google.common.collect.Lists;
import org.apache.commons.dbutils.DbUtils;
import org.apache.hadoop.util.ThreadUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertFalse;

/**
 * Provides high level query operations via jdbc.
 *
 * Modeled after the spring-framework class of the same name.
 */
public class JDBCTemplate {

    private Connection connection;

    public JDBCTemplate(Connection connection) {
        this.connection = connection;
    }

    /**
     * Query for a list of objects.  Example:
     *
     * <pre>
     *      List<Car> results = query("select * from CAR where color=? and year=?", new CarRowMapper(), "red", 2014);
     * </pre>
     */
    public <T> List<T> query(String sql, final RowMapper<T> rowMapper, Object... args) {
        return execute(new ResultSetExtractor<List<T>>() {
            @Override
            public List<T> extractData(ResultSet resultSet) throws SQLException {
                List<T> resultList = Lists.newArrayList();
                while (resultSet.next()) {
                    resultList.add(rowMapper.map(resultSet));
                }
                return resultList;
            }
        }, sql, args);
    }

    /**
     * Query for a list of objects.  Example:
     *
     * <pre>
     *      Car car = queryForObject("select * from CAR where color=? and year=?", new CarRowMapper(), "red", 2014);
     * </pre>
     */
    public <T> T queryForObject(final String sql, final RowMapper<T> rowMapper, Object... args) {
        return execute(new ResultSetExtractor<T>() {
            @Override
            public T extractData(ResultSet resultSet) throws SQLException {
                T result = null;
                if (resultSet.next()) {
                    result = rowMapper.map(resultSet);
                    assertFalse("only expected on row result for sql=" + sql, resultSet.next());
                }
                return result;
            }
        }, sql, args);
    }

    /**
     * Same as query() method in this class, but retries for up to the specified time.
     */
    public <T> List<T> queryWithWait(long waitTime, TimeUnit waitUnit, String sql, RowMapper<T> rowMapper, Object... args) throws SQLException {
        long startTime = currentTimeMillis();
        do {
            List<T> histories = query(sql, rowMapper, args);
            if (!histories.isEmpty()) {
                return histories;
            }
            ThreadUtil.sleepAtLeastIgnoreInterrupts(250);
        } while ((currentTimeMillis() - startTime) < waitUnit.toMillis(waitTime));

        return Lists.newArrayList();
    }

    /**
     * Same as queryForObject() method in this class, but retries for up to the specified time.
     */
    public <T> T queryForObjectWithWait(long waitTime, TimeUnit waitUnit, String sql, RowMapper<T> rowMapper, Object... args) throws SQLException {
        long startTime = currentTimeMillis();
        do {
            T histories = queryForObject(sql, rowMapper, args);
            if (histories != null) {
                return histories;
            }
            ThreadUtil.sleepAtLeastIgnoreInterrupts(250);
        } while ((currentTimeMillis() - startTime) < waitUnit.toMillis(waitTime));

        return null;
    }

    /**
     * Pass sql, parameters, and a ResultSetExtractor and this method takes care of converting exception to unchecked
     * and closing resultSet and statement.
     */
    public <T> T execute(ResultSetExtractor<T> preparedStatementCallback, String sql, Object... args) {
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            setArgs(preparedStatement, args);
            resultSet = preparedStatement.executeQuery();
            return preparedStatementCallback.extractData(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(resultSet);
            DbUtils.closeQuietly(preparedStatement);
        }
    }

    private void setArgs(PreparedStatement preparedStatement, Object[] args) throws SQLException {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                preparedStatement.setObject(i + 1, args[i]);
            }
        }
    }

    public static interface ResultSetExtractor<T> {
        T extractData(ResultSet rs) throws SQLException;
    }

}
