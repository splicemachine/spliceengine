/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.stream;

import com.splicemachine.access.HConfiguration;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Handles writing and loading operations to the file system for streaming message queues.
 */
public class HdfsStreamWriter {
    private static final Logger LOG = Logger.getLogger(HdfsStreamWriter.class);

    private static final String BACKUP_DIR = "stream_result";

    private Configuration conf;
    private UUID uuid;

    public HdfsStreamWriter(UUID uuid) {
        this.uuid = uuid;
        this.conf = HConfiguration.unwrapDelegate();
    }

    public void writeResults(int partition, Queue<Object> queue) throws IOException {
        FileSystem fs = FSUtils.getCurrentFileSystem(conf);

        Path backupFile = getFilePath(uuid, partition);

        if (fs.exists(backupFile)) {
            fs.delete(backupFile, true);
        }
        try(FSDataOutputStream out = fs.create(backupFile)) {
            out.write(SerializationUtils.serialize((Serializable) queue));
        }
    }

    public ArrayBlockingQueue<Object> readResults(int partition) throws IOException {
        FileSystem fs = FSUtils.getCurrentFileSystem(conf);

        Path backupFile = getFilePath(uuid, partition);

        if (!fs.exists(backupFile)) {
            return null;
        }

        ArrayBlockingQueue<Object>queue;
        try (FSDataInputStream in = fs.open(backupFile)) {
            queue = SerializationUtils.deserialize(in);
        }
        fs.delete(backupFile, true);

        return queue;
    }

    private Path getFilePath(UUID uuid, int partition) throws IOException {
        return new Path(getFileDir(uuid), String.valueOf(partition));
    }

    private Path getFileDir(UUID uuid) throws IOException {
        Path root = FSUtils.getRootDir(conf);
        Path backupDir = new Path(root, BACKUP_DIR);
        return new Path(backupDir, uuid.toString());
    }

    public void close() {
        try {
            FileSystem fs = FSUtils.getCurrentFileSystem(conf);
            fs.delete(getFileDir(uuid), true);
        } catch (Exception e) {
            LOG.warn(String.format("Could not remove message backup files for %s", uuid.toString()), e);
        }
    }

}
