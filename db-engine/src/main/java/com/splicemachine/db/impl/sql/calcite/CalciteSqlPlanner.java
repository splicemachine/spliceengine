package com.splicemachine.db.impl.sql.calcite;

/**
 * Created by yxia on 8/16/19.
 */

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.calcite.reloperators.SpliceImplementor;
import com.splicemachine.db.impl.sql.calcite.reloperators.SpliceRelNode;
import com.splicemachine.db.impl.sql.calcite.rules.SpliceConverterRule;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
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
    private SpliceContext sc;

    public CalciteSqlPlanner(SpliceContext spliceContext) {
        this.sc = spliceContext;
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
       // String plan = RelOptUtil.toString(tree);
       // return plan;

    }

    public RelNode derby2Rel(String sql, ResultSetNode resultSetNode) throws StandardException {
        RelNode root = relBuilder.convertResultSet(resultSetNode);
        String plan =  RelOptUtil.toString(root);
        if (LOG.isDebugEnabled()){
            LOG.debug(String.format("Plan for query <<\n\t%s\n>>\n%s\n",
                    sql,plan));
        }
        return root;
    }

    public RelNode optimize(String sql, RelNode root) {
        final Program program = Programs.standard();
        RelTraitSet desiredTraits = root.getTraitSet()
                .replace(SpliceRelNode.CONVENTION);
        RelOptPlanner optPlanner = root.getCluster().getPlanner();
        for (RelOptRule rule : SpliceConverterRule.RULES) {
            optPlanner.addRule(rule);
        }

        /* add rules related to Splice conventions */
        /*
        final RelVisitor visitor = new RelVisitor() {
            @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    final RelOptCluster cluster = node.getCluster();
                    final RelOptTable.ToRelContext context =
                            ViewExpanders.simpleContext(cluster);
                    final RelNode r = node.getTable().toRel(context);
                    optPlanner.registerClass(r);
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(root);
        */
        final RelNode rootRel4 = program.run(optPlanner,
                root, desiredTraits, ImmutableList.of(), ImmutableList.of());
        String plan = RelOptUtil.toString(rootRel4 /*, SqlExplainLevel.ALL_ATTRIBUTES */);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Plan after optimization for query <<\n\t%s\n>>\n%s\n",
                    sql, plan));
        }
        return rootRel4;
    }

    public QueryTreeNode implement(RelNode root) throws StandardException {
        SpliceImplementor implementor = new SpliceImplementor(sc);
        QueryTreeNode result = implementor.visitChild(0, root);
        return result;
    }
}
