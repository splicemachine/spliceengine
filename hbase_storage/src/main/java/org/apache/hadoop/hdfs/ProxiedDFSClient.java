/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProxiedDFSClient extends DFSClient {
    private final Connection connection;

    public ProxiedDFSClient(URI nameNodeUri, Configuration conf, FileSystem.Statistics stats, Connection connection) throws IOException {
        super(nameNodeUri, conf, stats);
        this.connection = connection;
    }

    @Override
    public LocatedBlocks getLocatedBlocks(String src, long start, long length) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_HDFS_OPERATION(?, ?)")) {
                statement.setString(1, src);
                statement.setString(2, "blocks");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for getFileStatus");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    HdfsProtos.LocatedBlocksProto lbp = HdfsProtos.LocatedBlocksProto.parseFrom(bytes);

                    return PBHelper.convert(lbp);
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
