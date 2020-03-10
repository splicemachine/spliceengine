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

package com.splicemachine.access.hbase;

import com.splicemachine.access.api.FilesystemAdmin;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.SerializableWritable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HFilesystemAdmin implements FilesystemAdmin {

    private final Admin admin;

    public HFilesystemAdmin(Admin admin) {
        this.admin = admin;
    }

    @Override
    public List<byte[]> hdfsOperation(String spath, String operation) throws IOException {
        Configuration conf = admin.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path rootDir = FSUtils.getRootDir(conf);
        Path path = new Path(spath);

        if (!path.toString().equals(spath)) {
            throw new IllegalArgumentException("Path is not canonical: " + spath);
        }

        switch (operation) {
            case "tokens": {
                FileStatus status = fs.getFileStatus(path);
                BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
                List<byte[]> results = new ArrayList<>();
                for (BlockLocation bl : blockLocations) {
                    if (!(bl instanceof HdfsBlockLocation)) {
                        throw new UnsupportedOperationException("Unexpected block location of type " + bl.getClass());
                    }
                    HdfsBlockLocation hbl = (HdfsBlockLocation) bl;
                    LocatedBlock locatedBlock = hbl.getLocatedBlock();
// TODO                    HdfsProtos.LocatedBlockProto lbp = PBHelper.convert(locatedBlock);
//                    results.add(lbp.toByteArray());
                }
                return results;
            }
            case "status": {
                FileStatus status = fs.getFileStatus(path);
                byte[] bytes = toByteArray(status);  // deserialize with Writables.copyWritable
                return Arrays.asList(bytes);
            }
            case "exists": {
                boolean result = fs.exists(path);
                return Arrays.asList( new byte [] {(byte) (result ? 1 : 0)});
            }
            case "list": {
                FileStatus[] status = fs.listStatus(path);
                byte[] bytes = toByteArray(status);  // deserialize with Writables.copyWritable
                return Arrays.asList(bytes);
            }
            case "blocks": {
// TODO                return Arrays.asList(PBHelper.convert(HDFSUtil.getBlocks((DistributedFileSystem)fs, spath)).toByteArray());
                return Collections.emptyList();
            }
            case "decrypt": {
                KeyProvider.KeyVersion decrypted = HDFSUtil.decrypt((DistributedFileSystem) fs, spath);
                byte[] bytes = toByteArray(new Text(decrypted.getName()), new Text(decrypted.getVersionName()));  // deserialize with Writables.copyWritable
                return Arrays.asList(bytes, decrypted.getMaterial());
            }
        }
        return null;
    }

    @Override
    public String extractConglomerate(String path) {
        String rootDir = null;
        try {
            rootDir = new URI(admin.getConfiguration().get("hbase.rootdir")).getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        String prefix = rootDir + "/data/splice/";
        if (!path.startsWith(prefix)) {
            throw new IllegalArgumentException("File doesn't belong to a Splice region: " + path);
        }

        String subPath = path.substring(prefix.length());
        int end = subPath.indexOf('/');
        String conglomName = end == -1 ? subPath : subPath.substring(0, end);
        
        return conglomName;
    }

    private static byte[] toByteArray(Writable... writables) {
        final DataOutputBuffer out = new DataOutputBuffer();
        try {
            for(Writable w : writables) {
                w.write(out);
            }
            out.close();
        } catch (IOException e) {
            throw new RuntimeException("Fail to convert writables to a byte array",e);
        }
        byte[] bytes = out.getData();
        if (bytes.length == out.getLength()) {
            return bytes;
        }
        byte[] result = new byte[out.getLength()];
        System.arraycopy(bytes, 0, result, 0, out.getLength());
        return result;
    }
}
