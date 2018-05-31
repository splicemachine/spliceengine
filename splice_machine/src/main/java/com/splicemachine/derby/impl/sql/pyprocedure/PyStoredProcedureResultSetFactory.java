package com.splicemachine.derby.impl.sql.pyprocedure;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.derby.utils.BaseAdminProcedures;
import com.splicemachine.pipeline.Exceptions;
import org.python.core.PyList;
import org.python.core.PyTuple;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.splicemachine.derby.utils.BaseAdminProcedures.getDefaultConn;

public class PyStoredProcedureResultSetFactory{

    public static EmbedResultSet create(PyList resultTuple)
            throws SQLException, Throwable {
        try {
            EmbedConnection conn = (EmbedConnection) getDefaultConn();
            Activation lastActivation = conn.getLanguageConnection().getLastActivation();

            // Each PyTuple contained in description describes the data type of a column
            // 0 - column name, 1 - jdbc type code,  2 - display sie, 3 - internal size, 4 - precision, 5 - scale, 6- nullablility
            PyList description = (PyList) resultTuple.get(0);
            // Each PyTuple contained in resultRows represents columns of data of a row
            PyList resultRows = (PyList) resultTuple.get(1);

            int colNum = description.size();
            final ResultColumnDescriptor[] descriptors = new GenericColumnDescriptor[colNum];
            for (int idx = 0; idx < colNum; ++idx) {
                DataTypeDescriptor descriptor;
                PyTuple pyDescriptor = (PyTuple) description.get(idx); // get(idx) will automatically do type coercion to java.lang.Object
                String colName = (String) pyDescriptor.get(0);
                Integer jdbcTypeId = (Integer) pyDescriptor.get(1);
                TypeId typeId = TypeId.getBuiltInTypeId((int) pyDescriptor.get(1));// convert jdbcId to TypeId
                boolean isNullable = ((int) pyDescriptor.get(6))>0?true:false;
                switch (jdbcTypeId){
                    case Types.BIGINT:
                    case Types.DECIMAL:
                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.INTEGER:
                    case Types.NUMERIC:
                    case Types.REAL:
                    case Types.SMALLINT:
                        Integer precision = (Integer) pyDescriptor.get(4);
                        Integer scale = (Integer) pyDescriptor.get(5);
                        precision = (precision == null)?typeId.getMaximumPrecision():precision;
                        scale = (scale==null)?typeId.getMaximumScale():scale;
                        descriptor = new DataTypeDescriptor(typeId, precision, scale, isNullable, typeId.getMaximumMaximumWidth());
                        break;
                    default:
                        descriptor = new DataTypeDescriptor(typeId, isNullable, typeId.getMaximumMaximumWidth());
                        break;
                }
                descriptors[idx] = new GenericColumnDescriptor(colName, descriptor);
            }

            ExecRow template = BaseAdminProcedures.buildExecRow(descriptors);
            List<ExecRow> rows = new ArrayList<>();
            if(resultRows.size() > 0){
                // construct MethodHandle Array
                template.resetRowArray();
                PyTuple templateRow = (PyTuple) resultRows.get(0); // Use the first row to construct MethodHandle array
                DataValueDescriptor[] templateDvds = template.getRowArray();
                MethodHandle[] mhs = new MethodHandle[colNum];
                for(int i = 0; i < colNum; ++i){
                    Object templateObj = templateRow.get(i);
                    MethodType mt = MethodType.methodType(void.class, templateObj.getClass());
                    mhs[i] =  MethodHandles.lookup().findVirtual(templateDvds[i].getClass(), "setValue", mt);
                }
                // fill each resultRow
                for (Object resultRow : resultRows) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    for (int idx = 0; idx < colNum; ++idx) {
                        Object currObj = ((PyTuple) resultRow).get(idx);
                        mhs[idx].invoke(dvds[idx], currObj);
                    }
                    rows.add(template.getClone());
                }
            }
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, descriptors, lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet result = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
            return result;
        } catch (StandardException se){
            throw PublicAPI.wrapStandardException(se);
        } catch (Exception e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

}