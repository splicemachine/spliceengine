package com.splicemachine.si.impl.compaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 11/25/15
 */
public class HFileDump{

    public static void main(String...args) throws Exception{
        Configuration conf=new Configuration();
        FileSystem fs =FileSystem.getLocal(conf);

        Path p = new Path(args[0]);
        HFile.Reader reader=HFile.createReader(fs,p,new CacheConfig(conf),conf);
        System.out.printf("Name: %s%n",reader.getName());
        System.out.printf("Path: %s%n",reader.getPath());
        System.out.printf("Compression Algorithm: %s%n",reader.getCompressionAlgorithm());
        System.out.printf("Data Block Encoding: %s%n",reader.getDataBlockEncoding());
        System.out.printf("Comparator: %s%n",reader.getComparator());
        System.out.printf("FileContext: %s%n",reader.getFileContext());
        System.out.printf("Has MVCC Info: %s%n",reader.hasMVCCInfo());
        System.out.printf("Length: %d%n",reader.length());
        System.out.printf("Entries: %d%n",reader.getEntries());
        System.out.printf("Index Size: %d%n",reader.indexSize());
        byte[] firstKey=reader.getFirstKey();
        if(firstKey==null){
            System.out.printf("Corrupt HFile: No First key present%n");
        }else{
            System.out.printf("First Key: %s%n",Bytes.toStringBinary(firstKey));
            System.out.printf("Mid Key: %s%n",Bytes.toStringBinary(reader.midkey()));
            System.out.printf("Last Key: %s%n",Bytes.toStringBinary(reader.getLastKey()));
        }
    }
}
