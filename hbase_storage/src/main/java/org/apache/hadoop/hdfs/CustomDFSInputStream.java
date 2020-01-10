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

/**
 * Created by jyuan on 4/9/19.
 */

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;

public class CustomDFSInputStream extends DFSInputStream {
    private String path;

    CustomDFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum, LocatedBlocks locatedBlocks) throws IOException, UnresolvedLinkException {
        super(dfsClient, src, verifyChecksum, locatedBlocks);
        this.path = src;
    }

    public String getPath() {
        return path;
    }
}
