package com.splicemachine.derby.impl.sql.compile.calcite;

/**
 * Created by yxia on 8/16/19.
 */

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.SqlPlanner;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceImplementor;
import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceJoin;
import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceRelNode;
import com.splicemachine.derby.impl.sql.compile.calcite.rules.SpliceConverterRule;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;
import org.apache.log4j.Logger;

import java.util.Collections;

import static org.apache.calcite.tools.Programs.sequence;

public class CalciteSqlPlanner implements SqlPlanner {
    public static Logger LOG = Logger.getLogger(CalciteSqlPlanner.class);
    private Planner planner;
    private CalciteConverterImpl relConverter;
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

        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(SpliceRelDataTypeSystemImpl.INSTANCE);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        RelOptPlanner planner = new VolcanoPlanner(config.getCostFactory(), spliceContext);
        RelOptUtil.registerDefaultRules(planner,
                false,
                false);
        final RexExecutorImpl executor = new RexExecutorImpl(Schemas.createDataContext(null, defaultSchema));
        planner.setExecutor(executor);
        //planner.setExecutor(config.getExecutor());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        }
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        CalciteCatalogReader catalogReader =
                new CalciteCatalogReader(CalciteSchema.from(defaultSchema).root(),
                        Collections.emptyList(),
                        typeFactory,
                        null);

      relConverter = new CalciteConverterImpl(spliceContext, cluster, catalogReader);
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

    private RelNode derby2Rel(ResultSetNode resultSetNode, String sql) throws StandardException {
        RelNode root = relConverter.convertResultSet(resultSetNode);
        if (LOG.isDebugEnabled()){
            String plan =  RelOptUtil.toString(root);
            LOG.debug(String.format("Plan for query <<\n\t%s\n>>\n%s\n",
                    sql,plan));
        }
        return root;
    }

    @Override
    public QueryTreeNode optimize(ResultSetNode resultSetNode, String sqlStmt) throws StandardException {
        RelNode root = derby2Rel(resultSetNode, sqlStmt);
        RelNode newRoot = optimize(root, sqlStmt);
        QueryTreeNode convertedTree = implement(newRoot);
        return convertedTree;
    }

    private RelNode optimize(RelNode root, String sqlStmt) {
        /* experiment with constant folding rule */
        Program constantFoldingProgram = constantFolding(DefaultRelMetadataProvider.INSTANCE);
        Program standardProgram = Programs.standard();
        Program program = sequence(constantFoldingProgram, standardProgram);

        RelTraitSet desiredTraits = root.getTraitSet()
                .replace(SpliceRelNode.CONVENTION);
        RelOptPlanner optPlanner = root.getCluster().getPlanner();
        for (RelOptRule rule : SpliceConverterRule.RULES) {
            optPlanner.addRule(rule);
        }

      //  optPlanner.addRule(SpliceFilterToProjectRule.INSTANCE);

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

        if (LOG.isDebugEnabled()) {
            String plan = RelOptUtil.toString(rootRel4 /*, SqlExplainLevel.ALL_ATTRIBUTES */);
            LOG.debug(String.format("Plan after optimization for query <<\n\t%s\n>>\n%s\n",
                    sqlStmt, plan));
        }
        return rootRel4;
    }

    private QueryTreeNode implement(RelNode root) throws StandardException {
        SpliceImplementor implementor = new SpliceImplementor(sc);
        boolean getPlan = false;
        if (root instanceof SpliceJoin)
            getPlan = true;
        if (true) {
            root = relConverter.getValuesStmtForPlan(root);
            root = optimize(root, "Plan");
        }
        QueryTreeNode result = implementor.visitChild(0, root);
        return result;
    }

    private Program constantFolding(RelMetadataProvider metadataProvider) {
        final HepProgramBuilder builder = HepProgram.builder();
        builder.addRuleCollection(ImmutableList.of(
                ValuesReduceRule.Config.FILTER.toRule(),
                ValuesReduceRule.Config.PROJECT_FILTER.toRule(),
                ValuesReduceRule.Config.PROJECT.toRule(),
          //      ReduceExpressionsRule.PROJECT_INSTANCE,
                ReduceExpressionsRule.CalcReduceExpressionsRule.Config.DEFAULT.toRule()
                ));
        return Programs.of(builder.build(), true, metadataProvider);
    }
}
