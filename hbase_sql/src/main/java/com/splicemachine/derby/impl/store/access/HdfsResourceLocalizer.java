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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.client.net.ResourceLocalizer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;

public class HdfsResourceLocalizer implements ResourceLocalizer {
    @Override
    public String copyToLocal(String resourcePath) throws IOException {
        java.nio.file.Path localPath = Files.createTempDirectory("localized-keytab", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));

        Path to = new Path(localPath.toString(), "keytab");
        Path from = new Path(resourcePath);

        FileSystem fs = FileSystem.get(from.toUri(), HConfiguration.unwrapDelegate());
        fs.copyToLocalFile(from, to);
        return to.toString();
    }
}
