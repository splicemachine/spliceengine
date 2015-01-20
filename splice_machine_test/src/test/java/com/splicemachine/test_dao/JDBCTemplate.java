package com.splicemachine.test_dao;

import com.google.common.collect.Lists;
import com.splicemachine.concurrent.Threads;
import org.apache.commons.dbutils.DbUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertFalse;

/**
 * Provides high level query operations via methods that do not throw checked exceptions and which handle low
 * level JDBC operations, such as creating and closing Statements and ResultSets, internally.
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
        return executeQuery(new ResultSetExtractor<List<T>>() {
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
     * Query for single objects.  Example:
     *
     * <pre>
     *      Car car = queryForObject("select * from CAR where color=? and year=?", new CarRowMapper(), "red", 2014);
     * </pre>
     */
    public <T> T queryForObject(final String sql, final RowMapper<T> rowMapper, Object... args) {
        return executeQuery(new ResultSetExtractor<T>() {
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
     * Get the results from a single column as a list.   Example:
     *
     * <pre>
     *     List<String> peopleNames = query("select name from person where age > ?", String.class, 21);
     * </pre>
     */
    public <T> List<T> query(String sql, Object... args) {
        return query(sql, new SingleColumnRowMapper<T>(), args);
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // update
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Pass sql, parameters, returns number of rows updated.
     */
    public int executeUpdate(String sql, Object... args) {
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            setArgs(preparedStatement, args);
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(resultSet);
            DbUtils.closeQuietly(preparedStatement);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // query retry and timeout
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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
            Threads.sleep(250, TimeUnit.MILLISECONDS);
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
            Threads.sleep(250, TimeUnit.MILLISECONDS);
        } while ((currentTimeMillis() - startTime) < waitUnit.toMillis(waitTime));

        return null;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // private
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Pass sql, parameters, and a ResultSetExtractor and this method takes care of converting exception to unchecked
     * and closing resultSet and statement.
     */
    private <T> T executeQuery(ResultSetExtractor<T> resultSetExtractor, String sql, Object... args) {
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            setArgs(preparedStatement, args);
            resultSet = preparedStatement.executeQuery();
            return resultSetExtractor.extractData(resultSet);
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

    /* implementations encapsulate how many rows to expect, what do do if there are too many rows, etc.  Public
     * RowMapper is used to transform each row into an object */
    private static interface ResultSetExtractor<T> {
        T extractData(ResultSet rs) throws SQLException;
    }

    private static class SingleColumnRowMapper<T> implements RowMapper<T> {
        @Override
        public T map(ResultSet resultSet) throws SQLException {
            return (T) resultSet.getObject(1);
        }
    }
}