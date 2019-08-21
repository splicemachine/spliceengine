package com.splicemachine.db.impl.sql;

/**
 * Created by yxia on 8/16/19.
 */

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;

public class CalciteSqlPlanner {
    public static final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private SchemaPlus defaultSchema;
    private Planner planner;

    public CalciteSqlPlanner(SpliceContext spliceContext) {
        defaultSchema = new SpliceSchema(spliceContext.getLcc(), "SPLICE");
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(defaultSchema)
                .context(spliceContext)
                .build();

        planner = Frameworks.getPlanner(config);
    }

    public String parse(String sql) throws SqlParseException, ValidationException, RelConversionException {
        SqlNode parse = planner.parse(sql);
        return parse.toString();
   //     SqlNode validate = planner.validate(parse);
   //     return validate.toString();
        /*
        RelNode tree = planner.convert(validate);

        String plan = RelOptUtil.toString(tree); //explain(tree, SqlExplainLevel.ALL_ATTRIBUTES);
        System.out.println("plan>");
        System.out.println(plan);
        */

    }
}
