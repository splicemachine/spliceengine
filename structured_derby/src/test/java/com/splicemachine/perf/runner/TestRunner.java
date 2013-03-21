package com.splicemachine.perf.runner;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.perf.runner.qualifiers.Qualifier;
import com.splicemachine.perf.runner.qualifiers.QualifierAdapter;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class TestRunner {
    private static enum RunMode{
        CREATE_ONLY(true, false,false,false),
        INSERT_ONLY(false,true, false,false),
        QUERY_ONLY (false,false,true, false),
        DROP_ONLY  (false,false,false,true),

        CREATE_AND_INSERT(true,true,false,false),
        CREATE_INSERT_AND_QUERY(true,true,true,false),
        CREATE_INSERT_AND_DROP(true,true,false,true),
        INSERT_AND_QUERY(false,true,true,false),
        RUN_ALL(true,true,true,true);

        private final boolean createPhase;
        private final boolean insertPhase;
        private final boolean queryPhase;
        private final boolean dropPhase;

        private RunMode(boolean createPhase, boolean insertPhase, boolean queryPhase, boolean dropPhase) {
            this.createPhase = createPhase;
            this.insertPhase = insertPhase;
            this.queryPhase = queryPhase;
            this.dropPhase = dropPhase;
        }

        public static RunMode getMode(boolean createPhase, boolean insertPhase, boolean queryPhase, boolean dropPhase) {
            for(RunMode runMode:values()){
                if(createPhase==runMode.createPhase){
                   if(insertPhase==runMode.insertPhase){
                       if(queryPhase==runMode.queryPhase){
                           if(dropPhase==runMode.dropPhase) return runMode;
                       }
                   }
                }
            }
            //default run mode is to run only the queries
            return QUERY_ONLY;
        }
    }
    public static void main(String...args) throws Exception{
        String dataFile = null;
        int pos=0;
        boolean createPhase = false;
        boolean insertPhase = false;
        boolean queryPhase = false;
        boolean dropPhase = false;
        while(pos<args.length){
            if(args[pos].equals("-t")){
                pos++;
                dataFile = args[pos];
                pos++;
            }else if("--drop".equalsIgnoreCase(args[pos])){
                pos++;
                dropPhase=true;
            }else if("--create".equalsIgnoreCase(args[pos])){
                pos++;
                createPhase = true;
            }else if("--all".equalsIgnoreCase(args[pos])){
                pos++;
                createPhase = true;
                insertPhase = true;
                queryPhase = true;
                dropPhase = true;
            }else if("--insert".equalsIgnoreCase(args[pos])){
                pos++;
                insertPhase = true;
            }else if("--query".equalsIgnoreCase(args[pos])){
                pos++;
                queryPhase=true;
            }
        }

        RunMode runMode = RunMode.getMode(createPhase, insertPhase, queryPhase, dropPhase);
        Preconditions.checkNotNull(dataFile,"No Data file specified");

        //noinspection ConstantConditions
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
            if(runMode.createPhase){
                data.createTables();
            }
            try{
                if(runMode.createPhase)
                    data.createIndices();
                if(runMode.insertPhase)
                    data.loadData();
                if(runMode.queryPhase){
                    data.runQueries();
                }
            }finally{
                if(runMode.dropPhase)
                    data.dropTables();
            }
        }finally{
            data.shutdown();
        }
    }
}
