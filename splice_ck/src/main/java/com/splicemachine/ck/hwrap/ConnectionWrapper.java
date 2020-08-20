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

package com.splicemachine.ck.hwrap;

import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static com.splicemachine.ck.Utils.tell;

public class ConnectionWrapper implements AutoCloseable {
    private Connection connection = null;
    private Configuration configuration = null;
    private Table table = null;

    public ConnectionWrapper() {}

    public ConnectionWrapper withConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public ConnectionWrapper connect() throws IOException {
        assert configuration != null;
        connection = ConnectionFactory.createConnection(configuration);
        return this;
    }

    public ConnectionWrapper withRegion(String region) throws IOException {
        assert connection != null;
        table = connection.getTable(TableName.valueOf(region));
        return this;
    }

    public ResultScanner scanSingleRowAllVersions(String key) throws IOException {
        assert table != null;
        Scan scan = new Scan();
        tell("hbase scan table", table.getName().toString(), "with all versions");
        scan.withStartRow(Bytes.fromHex(key)).setLimit(1).readAllVersions();
        return table.getScanner(scan);
    }

    public ResultScanner scanColumn(byte[] col) throws IOException {
        assert table != null;
        tell("hbase scan table", table.getName().toString(), "with projection");
        Scan scan = new Scan().addColumn(SIConstants.DEFAULT_FAMILY_BYTES, col);
        return table.getScanner(scan);
    }

    public ResultScanner scan() throws IOException {
        assert table != null;
        tell("hbase scan table", table.getName().toString());
        Scan scan = new Scan();
        return table.getScanner(scan);
    }

    public void put(Put put) throws IOException {
        assert table != null;
        tell("hbase put in table", table.getName().toString());
        table.put(put);
    }

    public List<TableDescriptor> descriptorsOfPattern(final String pattern) throws IOException {
        tell("hbase list table descriptors with pattern", pattern);
        return connection.getAdmin().listTableDescriptors(Pattern.compile(pattern));
    }

    @Override
    public void close() throws Exception {
        if(table != null) {
            table.close();
        }
        if(connection != null) {
            connection.close();
        }
    }
}
