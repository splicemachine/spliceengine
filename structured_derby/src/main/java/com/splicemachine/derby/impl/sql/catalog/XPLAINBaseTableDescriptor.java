package com.splicemachine.derby.impl.sql.catalog;

import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.util.IdUtil;

/**
 * Created by jyuan on 5/9/14.
 */
public abstract class XPLAINBaseTableDescriptor implements XPLAINTableDescriptor{

    protected abstract SystemColumn[] buildColumnList();

    protected abstract String[] getPrimaryKeys();

    public final String getTableDDL(String schemaName){
        String escapedSchema = IdUtil.normalToDelimited(schemaName);
        String escapedTableName = IdUtil.normalToDelimited(getTableName());
        StringBuilder queryBuilder = new StringBuilder("create table ");
        queryBuilder = queryBuilder.append(escapedSchema).append(".").append(escapedTableName);
        queryBuilder = queryBuilder.append("(");

        SystemColumn[]cols = buildColumnList();
        boolean isFirst=true;
        for(SystemColumn col:cols){
            if(!isFirst)queryBuilder = queryBuilder.append(",");
            else isFirst=false;

            queryBuilder = queryBuilder.append(col.getName()).append(" ");
            queryBuilder = queryBuilder.append(col.getType().getCatalogType().getSQLstring());
            if(!col.getType().isNullable()){
                queryBuilder = queryBuilder.append(" NOT NULL");
            }
        }
        String[] pks = getPrimaryKeys();
        if(pks!=null){
            queryBuilder = queryBuilder.append(", PRIMARY KEY(");

            isFirst=true;
            for(String pk:pks){
                if(!isFirst)queryBuilder = queryBuilder.append(",");
                else isFirst=false;
                queryBuilder = queryBuilder.append(pk);
            }
            queryBuilder = queryBuilder.append(")");
        }
        queryBuilder = queryBuilder.append(")");
        return queryBuilder.toString();
    }
}
