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

package com.splicemachine.derby.impl.load;

import splice.com.google.common.base.Joiner;

import java.io.*;

/**
 * System to generate fake data points into a file. This way we can write out quick,
 * well known files without storing a bunch of extras anywhere.
 *
 * @author Scott Fines
 *         Date: 10/20/14
 */
public class TestFileGenerator implements Closeable{
    private final File file;
    private BufferedWriter writer;
    private final Joiner joiner;

    public TestFileGenerator(String name) throws IOException {
        this.file = File.createTempFile(name,"csv");
        this.writer =  new BufferedWriter(new FileWriter(file));
        this.joiner = Joiner.on(",");
    }

    public TestFileGenerator row(String[] row) throws IOException {
        String line = joiner.join(row)+"\n";
        writer.write(line);
        return this;
    }

    public TestFileGenerator row(int[] row) throws IOException {
        String[] copy = new String[row.length];
        for(int i=0;i<row.length;i++){
            copy[i] = Integer.toString(row[i]);
        }
        return row(copy);
    }

    public String fileName(){
        return file.getAbsolutePath();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
