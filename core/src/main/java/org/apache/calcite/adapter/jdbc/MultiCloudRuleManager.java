package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;

public class MultiCloudRuleManager {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudRuleManager.class);

    public static List<RelOptRule> rules(JdbcConvention out) {
        return rules(out, RelFactories.LOGICAL_BUILDER);
    }

    public static List<RelOptRule> rules(JdbcConvention out, RelBuilderFactory relBuilderFactory) {
        return ImmutableList.of(
                //MultiCloudProjectRewriterRule.INSTANCE, //new JdbcToEnumerableConverterRule(out, relBuilderFactory)
                MultiCloudScanRewriterRule.INSTANCE
        );
    }

    /**
     * An example multi-cloud {@link RelOptRule} that triggers on {@link RelNode} match with operand {@link LogicalProject} that has a child operand {@link JdbcTableScan} and none after that.
     */
    public static class MultiCloudProjectRewriterRule extends RelOptRule {

        //~ Static fields/initializers ---------------------------------------------

        public static final RelOptRule INSTANCE =
                new MultiCloudProjectRewriterRule(RelFactories.LOGICAL_BUILDER);


        //~ Constructors -----------------------------------------------------------

        /**
         * Creates an example multi-cloud {@link MultiCloudProjectRewriterRule}.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public MultiCloudProjectRewriterRule(RelBuilderFactory relBuilderFactory) {
            super(operand(LogicalProject.class,
                    operand(JdbcTableScan.class, none())),
                    relBuilderFactory,
                    MultiCloudProjectRewriterRule.class.getSimpleName());
            logger.debug("INIT custom rule: " + this.getClass().getSimpleName());
        }

        //~ Methods ----------------------------------------------------------------
        @Override
        public void onMatch(RelOptRuleCall call) {
            RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

            logger.debug("MATCHED OPERAND");
            final LogicalProject project = call.rel(0);
            project.explain(rw);

            logger.debug("MATCHED CHILD OPERAND");
            final JdbcTableScan scan = call.rel(1);
            scan.explain(rw);

            logger.debug("TRANSFORMED OPERAND");
            final JdbcTableScan result = new JdbcTableScan(
                    scan.getCluster(),
                    scan.getTable(),
                    scan.jdbcTable, // protected
                    (JdbcConvention) scan.getConvention()
            );
            result.explain(rw);

            logger.debug("RESULT");
            call.transformTo(result);
        }
    }

    public static class MultiCloudScanRewriterRule extends RelOptRule {

        public static final RelOptRule INSTANCE =
                new MultiCloudScanRewriterRule(RelFactories.LOGICAL_BUILDER);


        public MultiCloudScanRewriterRule(RelBuilderFactory relBuilderFactory) {
            super(operand(JdbcTableScan.class, none()),
                    relBuilderFactory,
                    MultiCloudScanRewriterRule.class.getSimpleName());
            logger.debug("INIT custom rule: " + this.getClass().getSimpleName());
        }


        public void onMatch(RelOptRuleCall call) {
            RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

            logger.debug("MATCHED OPERAND");
            final JdbcTableScan originalScan = (JdbcTableScan) call.rels[0];
            originalScan.explain(rw);

            RelOptTable table = originalScan.getTable();
            String schemaName = table.getQualifiedName().get(0);
            String tableName = table.getQualifiedName().get(1);


            // transform the original scan only
            // fragment schema scans introduced as a result of original scan transformation have to be left intact
            if (schemaName.equals("mc_db") && tableName.equals("employees")) {
                RelBuilder builder = relBuilderFactory.create(originalScan.getCluster(), table.getRelOptSchema());
                RelNode multiCloudScan = builder
                        .scan("mc_db_google", "employees")
                        .project(builder.field("multiid"), builder.field("id"), builder.field("first"), builder.field("last"))
                        .scan("mc_db_amazon", "employees")
                        .project(builder.field("multiid"), builder.field("age"))
                        .join(JoinRelType.INNER, "multiid")
                        .project(builder.field("id"), builder.field("age"), builder.field("first"), builder.field("last"))
                        .build();

                logger.debug("RESULT");
                multiCloudScan.explain(rw);

                // TODO: Planner currently just goes through a lot of rules to transform the query BACK to original one. How to make this work?
                // trying to set importance to 0 of the original RelNode so that Planner ignores it
                //call.getPlanner().setImportance(call.rels[0], 0); // when importance is set to 0, pending rule calls are cancelled, and future rules will not fire.


                call.transformTo(multiCloudScan);
                logger.debug(call.getMetadataQuery().getCumulativeCost(multiCloudScan).toString());

            }
        }
    }
}