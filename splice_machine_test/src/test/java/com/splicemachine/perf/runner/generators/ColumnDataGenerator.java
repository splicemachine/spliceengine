package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Created on: 3/15/13
 */
public interface ColumnDataGenerator {

    public void setInto(PreparedStatement ps,int position) throws SQLException;

}
