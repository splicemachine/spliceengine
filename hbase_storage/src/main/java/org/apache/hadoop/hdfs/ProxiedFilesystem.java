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
 *
 */

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProxiedFilesystem extends DistributedFileSystem {

    private final DistributedFileSystem delegate;
    private Connection connection;

    public ProxiedFilesystem(DistributedFileSystem fs, String connectionURL) throws IOException {
        delegate = fs;
        try {
            connection = DriverManager.getConnection(connectionURL);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getScheme() {
        return super.getScheme();
    }

    @Override
    public URI getUri() {
        return super.getUri();
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        this.dfs.close();
        this.dfs = new ProxiedDFSClient(uri, conf, statistics, connection);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, file.getPath().toUri().getPath());
                statement.setString(2, "tokens");
                try (ResultSet rs = statement.executeQuery()) {
                    List<HdfsProtos.LocatedBlockProto> protos = new ArrayList<>();
                    while (rs.next()) {
                        Blob blob = rs.getBlob(1);
                        byte[] bytes = blob.getBytes(1, (int) blob.length());
                        HdfsProtos.LocatedBlockProto lbp = HdfsProtos.LocatedBlockProto.parseFrom(bytes);
                        protos.add(lbp);
                    }
//                     TODO return DFSUtil.locatedBlocks2Locations(PBHelper.convertLocatedBlock(protos));
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
        return getFileBlockLocations(getFileStatus(p), start, len);
    }

    @Override
    public FileStatus[] listStatus(Path p) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, p.toUri().getPath());
                statement.setString(2, "list");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for getFileStatus");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    List<FileStatus> results = new ArrayList<>();
                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                    try {
                        while (dis.available() > 0) {
                            FileStatus status = new FileStatus();
                            status.readFields(dis);
                            results.add(status);
                        }
                    } finally {
                        dis.close();
                    }
                    FileStatus[] result = new FileStatus[results.size()];
                    int i = 0;
                    for (FileStatus fs : results) {
                        result[i++] = fs;
                    }
                    return result;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, f.toUri().getPath());
                statement.setString(2, "status");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for getFileStatus");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    FileStatus status = new FileStatus();
                    Writables.copyWritable(bytes, status);
                    return status;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists(Path f) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, f.toUri().getPath());
                statement.setString(2, "exists");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for exists");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    if (bytes.length != 1) {
                        throw new IOException("Wrong response size for exists");
                    }
                    return bytes[0] == 1;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public FsStatus getStatus() throws IOException {
        return delegate.getStatus();
    }

    @Override
    public Configuration getConf() {
        return delegate.getConf();
    }
}
