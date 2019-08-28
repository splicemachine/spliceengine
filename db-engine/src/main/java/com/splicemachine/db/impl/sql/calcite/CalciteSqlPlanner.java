package com.splicemachine.db.impl.sql.calcite;

/**
 * Created by yxia on 8/16/19.
 */

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.SelectNode;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;
import org.apache.log4j.Logger;

import java.util.Collections;

public class CalciteSqlPlanner {
    public static Logger LOG = Logger.getLogger(CalciteSqlPlanner.class);
    private Planner planner;
    private DerbyToCalciteRelBuilder relBuilder;

    public CalciteSqlPlanner(SpliceContext spliceContext) {
        SpliceSchema spliceSchema = new SpliceSchema(spliceContext.getLcc(), "", null, true);
        SchemaPlus defaultSchema = CalciteSchema.createRootSchema(false, false, spliceSchema.getName(), spliceSchema).plus();
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(defaultSchema)
                .context(spliceContext)
                .build();

        planner = Frameworks.getPlanner(config);

        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        RelOptPlanner planner = new VolcanoPlanner(config.getCostFactory(), spliceContext);
        RelOptUtil.registerDefaultRules(planner,
                false,
                false);
        planner.setExecutor(config.getExecutor());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        CalciteCatalogReader catalogReader =
                new CalciteCatalogReader(CalciteSchema.from(defaultSchema).root(),
                        Collections.emptyList(),
                        typeFactory,
                        null);

        relBuilder = new DerbyToCalciteRelBuilder(spliceContext, cluster, catalogReader);
    }

    public String parse(String sql) throws SqlParseException, ValidationException, RelConversionException {
        // parse
        SqlNode parse = planner.parse(sql);
      //  return parse.toString();

        // validate
        SqlNode validate = planner.validate(parse);

        SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.DERBY)
                .withIdentifierQuoteString("\"");

        String sqlString = validate.toSqlString(new DerbySqlDialect(c)).getSql();
        return sqlString;

        // convert to RelOptTree
       // RelNode tree = planner.rel(validate).rel;
       // String plan = RelOptUtil.toString(tree); //explain(tree, SqlExplainLevel.ALL_ATTRIBUTES);
       // return plan;

    }

    public String derby2Rel(String sql, SelectNode selectNode) throws StandardException {
        /*
        RelNode node = relBuilder.scan("SPLICE", "T1")
                .project(relBuilder.field("C1"), relBuilder.field("B1")).build();
        */

        RelNode root = relBuilder.convertSelect(selectNode);
        String plan =  RelOptUtil.toString(root);
        if (LOG.isDebugEnabled()){
            LOG.info(String.format("Plan for query <<\n\t%s\n>>\n%s\n",
                    sql,plan));
        }
        return plan;
    }
}
