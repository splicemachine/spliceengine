package com.splicemachine.perf.runner;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splicemachine.perf.runner.qualifiers.Qualifier;
import com.splicemachine.perf.runner.qualifiers.QualifierAdapter;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class TestRunner {

    public static void main(String...args) throws Exception{
        String dataFile = null;
        int pos=0;
        while(pos<args.length-1){
            if(args[pos].equals("-t")){
                pos++;
                dataFile = args[pos];
                pos++;
            }
        }

        Preconditions.checkNotNull(dataFile,"No Data file specified");

        if(!dataFile.startsWith("/")){
            dataFile = System.getProperty("user.dir")+"/"+dataFile;
        }

        File file = new File(dataFile);
        Preconditions.checkArgument(file.exists(),"File "+ dataFile+" not found");

        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(Column.class,new Column.ColumnAdapter());
        builder.registerTypeAdapter(Qualifier.class,new QualifierAdapter());
        Gson gson = builder.create();

        Reader reader = new FileReader(file);
        Data data = gson.fromJson(reader, Data.class);
        System.out.println(data);

        data.connect();
        try{
            data.createTables();
            try{
                data.loadData();
                data.runQueries();
            }finally{
                data.dropTables();
            }
        }finally{
            data.shutdown();
        }
    }
}
