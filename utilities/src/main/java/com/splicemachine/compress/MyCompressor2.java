package com.splicemachine.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyCompressor2 {
    int CSV_FILE_MAX = 10*1024*1024;
    FileOutputStream out;
    String csvFile;
    String path;
    List<Type> types;

    public enum Type {
        INTEGER(0),
        DOUBLE(1),
        STRING(2),
        TIMESTAMP(3);

        int id;

        public int getId() { return id; }
        Type(int id) {
            this.id = id;
        }

        public static Type getType(Object o) {
            if(o instanceof Integer) {
                return INTEGER;
            }
            else if(o instanceof Double) {
                return DOUBLE;
            }
            else if(o instanceof String) {
                return STRING;
            }
            else if(o instanceof Timestamp) {
                return TIMESTAMP;
            }
            throw new RuntimeException("unsupported type " + o.getClass().getName());
        }

        public Object parse(String s) {
            switch(this) {
                case DOUBLE:
                    return Double.parseDouble(s);
                case INTEGER:
                    return Integer.parseInt(s);
                case STRING:
                    return s.substring(1, s.length()-1);
                case TIMESTAMP:
                    return Timestamp.valueOf(s);
                default:
                    throw new RuntimeException("not implemented");
            }
        }

        public TypeDescription getOrcTypeDescription() {
            switch(this) {
                case DOUBLE: return TypeDescription.createDouble();
                case INTEGER: return TypeDescription.createInt();
                case STRING: return TypeDescription.createString();
                case TIMESTAMP: return TypeDescription.createTimestamp();
                default:
                    throw new RuntimeException("not implemented");
            }
        }
    }

    public MyCompressor2(String path, List<Type> types) throws FileNotFoundException {
        this.path = path;
        this.csvFile = path + "/tmp.csv";
        this.types = types;
        out = new FileOutputStream(csvFile, true /* append*/);

    }

    public void addAll(List<List<Object>> objs) throws IOException {
        for(List<Object> obj : objs)
            addL(obj, false);
        flush();
    }

    void flush() throws IOException {
        out.flush();
        if (getFileSize(csvFile) > CSV_FILE_MAX) {
            doCsvOverflow();
        }
    }

    void writeToCsv(List<Object> objs) throws IOException {

        if(types == null) {
            types = new ArrayList<>(objs.size());
            for(Object o : objs) {
                assert(o != null); // then we would need real type information.
                types.add(Type.getType(o));
            }

        }
        else {
            assert objs.size() == types.size();
            for (int i = 0; i < objs.size(); i++) {
                assert objs.get(i) == null || Type.getType(objs.get(i)) == types.get(i);
            }
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(Object o : objs) {
            if(!first)
                sb.append(";");
            else
                first = false;
            if(o != null) {
                if (o instanceof String) {
                    String s = (String) o;
                    assert s.lastIndexOf(";") == -1;
                    s.replace("\\", "\\\\");
                    s.replace("\"", "\\\"");
                    sb.append("\"" + s + "\"");
                }
                else
                    sb.append(o.toString());
            }
        }
        sb.append("\n");
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    void close() throws IOException {
        out.close();
    }

    static List<List<Object>> readCsv(String filepath, List<Type> types, String separator) throws IOException {
        List<List<Object>> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(separator);

                List<Object> row = new ArrayList<>();
//                if(values.length != types.size() )
//                    throw new RuntimeException("error in line " + line + " " + Arrays.toString(values));
                for(int i=0; i<types.size(); i++) {
                    if(values.length <= i || values[i].length()==0)
                        //row.add(null); // todo
                        row.add( new Integer(0) );
                    else
                        row.add(types.get(i).parse(values[i]));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    void add(Object ...objs) throws IOException {
        addL(Arrays.asList(objs) );
    }

    long getFileSize(String path)
    {
        File f = new File(path);
        if(f.exists())
            return f.length();
        return 0;
    }

    void getIntegerColumnVector(List<List<Object>> rows, int col,
                                VectorizedRowBatch batch, int from, int to) {
        LongColumnVector dcv = (LongColumnVector) batch.cols[col];
        for(int r=from; r<to; r++) {
            dcv.vector[r-from] = ((Integer)rows.get(r).get(col)).longValue();
        }
    }

    void getDoubleColumnVector(List<List<Object>> rows, int col,
                               VectorizedRowBatch batch, int from, int to) {
        DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[col];
        for(int r=from; r<to; r++) {
            dcv.vector[r-from] = ((Double)rows.get(r).get(col)).doubleValue();
        }
    }

    void getTimestampColumnVector(List<List<Object>> rows, int col,
                               VectorizedRowBatch batch, int from, int to) {
        TimestampColumnVector dcv = (TimestampColumnVector) batch.cols[col];
        for(int r=from; r<to; r++) {
            Timestamp ts = ((Timestamp)rows.get(r).get(col));
            dcv.time[r-from] = ts.getTime();
            dcv.nanos[r-from] = ts.getNanos();
        }
    }

    void getStringColumnVector(List<List<Object>> rows, int col,
                                  VectorizedRowBatch batch, int from, int to) {

        BytesColumnVector bcv = (BytesColumnVector) batch.cols[1];
        byte[] EMPTY_BYTES = "".getBytes(StandardCharsets.UTF_8);
        for(int r=from; r<to; r++) {

//          vName.setRef(i, EMPTY_BYTES, 0, 0);
//            vName.isNull[i] = true;
//            vName.noNulls = false; // todo

            String s = ((String)rows.get(r).get(col));
            bcv.setVal(r-from, s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public class orcCompactThread extends Thread {

        public void run(){
            System.out.println("MyThread running");
        }
    }

    List<List<Object>> rows = new ArrayList<>();

    void doCsvOverflow() throws IOException {
        Configuration conf = new Configuration();
//        List<List<Object>> rows = readCsv(csvFile, types, ";");
        long csvSize = getFileSize(csvFile);

        String orcPath = getNewOrcFilename();

        TypeDescription schema = TypeDescription.createStruct();
        for(int i=0; i<types.size(); i++) {
            schema.addField( "_col" + i, types.get(i).getOrcTypeDescription());
        }

        Writer writer = OrcFile.createWriter(new Path(orcPath),
                        OrcFile.writerOptions(conf).setSchema(schema));

        int x = 0;
        VectorizedRowBatch batch = schema.createRowBatch();
        final int BATCH_SIZE = batch.getMaxSize();

        while(rows.size()-x > 0) {
            int currentBatchSize = rows.size() - x;
            if(currentBatchSize > BATCH_SIZE) currentBatchSize = BATCH_SIZE;
            batch.size = currentBatchSize;

            int from = x;
            int to = x+currentBatchSize;

            for(int col=0; col<types.size(); col++) {
                switch(types.get(col)) {
                    case DOUBLE:
                        getDoubleColumnVector(rows, col, batch, from, to);
                        break;
                    case INTEGER:
                        getIntegerColumnVector(rows, col, batch, from, to);
                        break;
                    case STRING:
                        getStringColumnVector(rows, col, batch, from, to);
                        break;
                    case TIMESTAMP:
                        getTimestampColumnVector(rows, col, batch, from, to);
                        break;
                }
            }

            writer.addRowBatch(batch);
            batch.reset();
            x += currentBatchSize;
        }

        writer.close();
        long orcSize = getFileSize(orcPath);
        System.out.println("Compression factor " + csvSize/(double)orcSize);
        out.close();
        out = new FileOutputStream(csvFile, false /* no append, renew*/);
        rows.clear();
    }

    private String getNewOrcFilename() {
        String orcPath = "";
        for(int i=0; i<10000; i++) {
             orcPath = path + "/output_" + i + ".orc";
             if(!new File(orcPath).exists())
                 break;
        }
        return orcPath;
    }

    void addL(List<Object> objs, boolean flush) throws IOException {
        rows.add(objs);
        writeToCsv(objs);
        if(flush) {
            flush();
        }
    }
    void addL(List<Object> objs) throws IOException {
        addL(objs, true);
    }
}
