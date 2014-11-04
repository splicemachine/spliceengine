package com.splicemachine.derby.impl.job.load;

import com.google.common.base.Joiner;

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
