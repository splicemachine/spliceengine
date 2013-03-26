package com.splicemachine.perf.runner;

import com.google.common.base.Preconditions;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.splicemachine.perf.runner.generators.ColumnDataGenerator;
import com.splicemachine.perf.runner.generators.Generators;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class Column {
    private final String name;
    private final ColumnType type;
    private final boolean isPrimaryKey;
    private final ColumnDataGenerator dataGenerator;
    private final int width;
    private boolean primaryKey;

    public Column(String name, ColumnType type, boolean primaryKey, ColumnDataGenerator dataGenerator,int width) {
        this.name = name;
        this.type = type;
        isPrimaryKey = primaryKey;
        this.dataGenerator = dataGenerator;
        this.width = width;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", isPrimaryKey=" + isPrimaryKey +
                ", dataGenerator=" + dataGenerator +
                ", width=" + width +
                '}';
    }

    public String getSqlRep() {
        StringBuilder sb = new StringBuilder(name).append(" ") ;
        sb = sb.append(type.getTypeName());
        if(type.requiresWidth()){
            sb = sb.append("(").append(width).append(")");
        }
        return sb.toString();
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public String getName() {
        return name;
    }

    public void setData(PreparedStatement ps, int pos) throws SQLException {
        dataGenerator.setInto(ps,pos);
    }

    public static class Builder{
        private String name;
        private ColumnType type;
        private boolean primaryKey = false;
        private ColumnDataGenerator dataGenerator;
        private int width = -1;

        public Builder name(String name){
            this.name = name;
            return this;
        }

        public Builder primaryKey(boolean isPrimaryKey){
            this.primaryKey = isPrimaryKey;
            return this;
        }

        public Builder type(String typeName){
            this.type = ColumnType.parse(typeName);
            return this;
        }

        public Builder generatorType(ColumnDataGenerator dataGenerator){
            this.dataGenerator = dataGenerator;
            return this;
        }

        public Builder width(int width) {
            this.width = width;
            return this;
        }

        public Column build(){
            Preconditions.checkNotNull(name,"No Column name specified!");
            Preconditions.checkNotNull(type,"No Column type specified!");
//            Preconditions.checkNotNull(dataGenerator,"No Data Generator specified!");
            if(type.requiresWidth())
                Preconditions.checkArgument(width > 0, "No Width specified!");

            return new Column(name,type, primaryKey, dataGenerator,width);
        }

    }

    public static class ColumnAdapter extends TypeAdapter<Column>{

        @Override public void write(JsonWriter out, Column value) throws IOException { }

        @Override
        public Column read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            in.beginObject();
            Column.Builder colBuilder = new Builder();
            TypeAdapter<? extends ColumnDataGenerator> dataGen = null;
            while(in.hasNext()){
                String fieldName = in.nextName();
                if(fieldName.equalsIgnoreCase("name")){
                    colBuilder.name(in.nextString());
                }else if(fieldName.equalsIgnoreCase("type")){
                    colBuilder.type(in.nextString());
                }else if(fieldName.equalsIgnoreCase("primaryKey"))
                    colBuilder.primaryKey(in.nextBoolean());
                else if(fieldName.equalsIgnoreCase("generatorType")){
                    dataGen = Generators.getGenerator(in.nextString());
                }else if(fieldName.equalsIgnoreCase("generatorConfig")){
                    //TODO -sf- requires "generatorConfig" to come after "generatorType", will want to fix eventually
                    colBuilder.generatorType(dataGen.read(in));
                }else if(fieldName.equalsIgnoreCase("width"))
                    colBuilder.width(in.nextInt());
                else{
                    switch (in.peek()) {
                        case BEGIN_ARRAY:
                            in.beginArray();
                            break;
                        case END_ARRAY:
                            in.endArray();
                            break;
                        case BEGIN_OBJECT:
                            in.beginObject();
                            break;
                        case END_OBJECT:
                            in.endObject();
                            break;
                        case NAME:
                            in.nextName();
                            break;
                        case STRING:
                            in.nextString();
                            break;
                        case NUMBER:
                            in.nextInt();
                            break;
                        case BOOLEAN:
                            in.nextBoolean();
                            break;
                        case NULL:
                            in.nextNull();
                            break;
                        case END_DOCUMENT:
                            break;
                    }
                }
            }
            in.endObject();
            return colBuilder.build();
        }
    }
}
