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

package com.splicemachine.derby.impl.io;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.impl.spark.WholeTextInputFormat;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Scott Fines
 *         Date: 8/4/16
 */
public class WholeTextInputFormatTest{

    @Test
    public void testGetsStreamForDirectory() throws Exception{
        /*
         * This test failed before changes to WholeTextInputFormat(hooray for test-driven development!),
         * so this constitutes an effective regression test for SPLICE-739. Of course, we'll be certain
         * about it by ALSO writing an IT, but this is a nice little Unit test of the same thing.
         */
        Configuration configuration =HConfiguration.unwrapDelegate();
        String dirPath =SpliceUnitTest.getResourceDirectory()+"multiLineDirectory";
        configuration.set("mapred.input.dir",dirPath);

        WholeTextInputFormat wtif = new WholeTextInputFormat();
        wtif.setConf(configuration);

        JobContext ctx = new JobContextImpl(configuration,new JobID("test",1));
        List<InputSplit> splits=wtif.getSplits(ctx);

        int i=0;
        Set<String> files = readFileNames(dirPath);

        Assert.assertEquals("We didn't get a split per file", files.size(), splits.size());

        Set<String> readFiles = new HashSet<>();
        long totalRecords = 0;

        for(InputSplit is:splits){
            TaskAttemptContext tac = new TaskAttemptContextImpl(configuration,new TaskAttemptID("test",1,TaskType.MAP,i,1));
            RecordReader<String, InputStream> recordReader=wtif.createRecordReader(is,tac);
            CombineFileSplit cfs = (CombineFileSplit)is;
            System.out.println(cfs);

            totalRecords += collectRecords(readFiles,recordReader);
            i++;
        }
        Assert.assertEquals("did not read all data!",28,totalRecords);

        Assert.assertEquals("Did not read all files!",files.size(),readFiles.size());
        for(String expectedFile:files){
            Assert.assertTrue("Did not read file <"+expectedFile+"> read =" + readFiles + " exp",readFiles.contains(expectedFile));
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private Set<String> readFileNames(String dirPath){
        Set<String> fileNames = new HashSet<>();
        File d = new File(dirPath);
        //we can assume that d exists, but let's be safe
        Assert.assertTrue("Programmer error: missing directory <"+dirPath+">",d.exists());
        Assert.assertTrue("Programmer error: not a directory <"+dirPath+">",d.isDirectory());
        File[] files=d.listFiles();
        Assert.assertNotNull("Programmer error: did not list files properly <"+dirPath+">",files);
        for(File file:files){
            //CombineFileSplit prepends the file: to the front of all of its paths, so we need to do the same for equality checking
            fileNames.add("file:"+file.getAbsolutePath());
        }
        return fileNames;
    }

    private long collectRecords(Set<String> fileNames,RecordReader<String, InputStream> recordReader) throws IOException, InterruptedException{
        long count = 0L;
        while(recordReader.nextKeyValue()){
            String key = recordReader.getCurrentKey();
            key = key.replaceAll("/+", "/"); // some platforms add more "/" at the beginning, coalesce them for equality check
            Assert.assertTrue("Seen the same file twice!",fileNames.add(key));

            InputStream is = recordReader.getCurrentValue();
            try(BufferedReader br = new BufferedReader(new InputStreamReader(is))){
                String n;
                while((n = br.readLine())!=null){
                   count++;
                }
            }
        }
        return count;
    }
}
