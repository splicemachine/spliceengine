/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.load;

import org.spark_project.guava.base.Joiner;

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
