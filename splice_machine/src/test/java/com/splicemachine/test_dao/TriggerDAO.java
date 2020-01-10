/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.test_dao;

import org.junit.Assert;

import java.sql.Connection;
import java.util.List;

/**
 * Query sys.systriggers.
 */
public class TriggerDAO {

    private JDBCTemplate jdbcTemplate;

    public TriggerDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
    }

    /**
     * Count number of defined triggers with the specified name.
     */
    public long count(String triggerName) {
        List<Long> count = jdbcTemplate.query("" +
                "select count(*) from sys.systriggers t " +
                "join sys.sysschemas s on s.schemaid=t.schemaid " +
                "where triggername=? and schemaname=CURRENT SCHEMA", triggerName.toUpperCase());
        return count.get(0);
    }

    /**
     * Throws assertion error if the specified trigger does not exist in the current schema.
     */
    public void assertTriggerExists(String... triggerNames) {
        for (String triggerName : triggerNames) {
            Assert.assertTrue("expected trigger to exist = " + triggerName, count(triggerName) == 1);
        }
    }

    /**
     * Throws assertion error if the specified trigger does not exist in the current schema.
     */
    public void assertTriggerGone(String... triggerNames) {
        for (String triggerName : triggerNames) {
            Assert.assertTrue("expected trigger NOT to exist = " + triggerName, count(triggerName) == 0);
        }
    }

    /**
     * Drop the specified triggers.
     */
    public void drop(String... triggerNames) {
        for (String triggerName : triggerNames) {
            jdbcTemplate.executeUpdate("DROP TRIGGER " + triggerName);
        }
    }

    /**
     * Returns the names of all triggers on the specified table
     */
    public List<String> getTriggerNames(String schemaName, String tableName) {
        return jdbcTemplate.query("" +
                "select trig.triggername " +
                "from sys.systriggers trig " +
                "join sys.sysschemas  s    on trig.schemaid = s.schemaid " +
                "join sys.systables   tab  on trig.tableid  = tab.tableid " +
                "where s.schemaname=? and tab.tablename=?", schemaName.toUpperCase(), tableName.toUpperCase());
    }

    /**
     * Drop all triggers on the specified table.
     */
    public void dropAllTriggers(String schemaName, String tableName) {
        for (String triggerName : getTriggerNames(schemaName, tableName)) {
            drop(triggerName);
        }
    }

}
