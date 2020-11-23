package com.splicemachine.ck.command.common;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.derby.utils.EngineUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.sql.SQLException;

public class TableNameGroup {
    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
    public OptionallyQualifiedTableName qualifiedTableName;

    @CommandLine.Option(names = {"-r", "--region"}, required = true, description = "HBase region name (with of without 'splice:' prefix)")
    public String region;

    public String getRegion(HBaseInspector hbaseInspector) throws Exception {
        if(qualifiedTableName != null) {
            qualifiedTableName.table = EngineUtils.validateTable(qualifiedTableName.table);
            qualifiedTableName.schema = EngineUtils.validateSchema(qualifiedTableName.schema);
            return hbaseInspector.regionOf(qualifiedTableName.schema, qualifiedTableName.table);
        } else {
            if(StringUtils.isNumeric(region)) {
                return "splice:" + region;
            } else {
                return region;
            }
        }
    }
}
