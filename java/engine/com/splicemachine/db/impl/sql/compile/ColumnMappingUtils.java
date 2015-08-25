package com.splicemachine.db.impl.sql.compile;

import java.util.Iterator;

public class ColumnMappingUtils {

    /**
     * Updates the result set number and the column id of columns found in colRefsToUpdate to
     * the value found at that column name in colsToUpdateFrom.
     *
     * @param colsToUpdateFrom
     * @param colRefsToUpdate
     */
    public static void updateColumnMappings(ResultColumnList colsToUpdateFrom, Iterator colRefsToUpdate) {

        while(colRefsToUpdate.hasNext()){

            ColumnReference colRef = (ColumnReference) colRefsToUpdate.next();
            ResultColumn rcToUpdate = colRef.getSource();

            if(rcToUpdate != null){
                ResultColumn updateFromRC = colsToUpdateFrom.getResultColumnFullName(rcToUpdate.getFullName(), true);
                if(updateFromRC != null){
                    rcToUpdate.setResultSetNumber(updateFromRC.getResultSetNumber());
                    rcToUpdate.setVirtualColumnId(updateFromRC.getVirtualColumnId());
                }

            }
        }
    }
}
