package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// used in comments
import org.apache.calcite.rel.core.*;

public class MultiCloudRuleManager {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudRuleManager.class);

    public static RuleSet rules() {
        return RuleSets.ofList(
                //MultiCloudScanRewriterRule.INSTANCE
        );
    }

    //~ Hook manager ---------------------------------------------

    /**
     * A manager for {@link Hook} features that enables {@link RuleSet} to be executed in heuristic {@link org.apache.calcite.plan.hep.HepPlanner} BEFORE cost-based {@link org.apache.calcite.plan.volcano.VolcanoPlanner} optimization.
     */
    public static class MultiCloudHookManager {
        private static final Program PROGRAM = new MultiCloudProgram();

        private static Hook.Closeable globalProgramClosable;

        public static void addHook() {
            if (globalProgramClosable == null) {
                globalProgramClosable = Hook.PROGRAM.add(program());
            }
        }

        private static Consumer<Holder<Program>> program() {
            return prepend(PROGRAM);
        }

        private static Consumer<Holder<Program>> prepend(Program program) { // this doesn't have to be in the separate program
            return (holder) -> {
                if (holder == null) {
                    throw new IllegalStateException("No program holder");
                }
                Program chain = holder.get();
                if (chain == null) {
                    chain = Programs.standard();
                }
                holder.set(Programs.sequence(program, chain));
            };
        }
    }

    //~ Custom rules ---------------------------------------------

    /**
     * A multi-cloud {@link RelOptRule} that triggers on {@link RelNode} match with operand {@link JdbcTableScan} that has no children.
     * <p>
     * Rewrites {@link JdbcTableScan} into multiple {@link TableScan}s with {@link Project} on each of them,
     * and then {@link Join}s them using 'multiid'. Finally, it {@link Project}s fields back into the original form
     * to return semantically equivalent {@link RelNode}.
     */
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

                call.transformTo(multiCloudScan);
                logger.debug(call.getMetadataQuery().getCumulativeCost(multiCloudScan).toString());
            }
        }
    }

    // TODO: Remove this rule when done with RelVisitor implementation
    public static class MultiCloudDataCollector extends RelOptRule {

        public static final RelOptRule INSTANCE =
                new MultiCloudDataCollector(RelFactories.LOGICAL_BUILDER);


        public MultiCloudDataCollector(RelBuilderFactory relBuilderFactory) {
            super(operand(LogicalProject.class,
                    operand(JdbcTableScan.class, none())),
                    relBuilderFactory,
                    MultiCloudDataCollector.class.getSimpleName());
            logger.debug("INIT custom rule: " + this.getClass().getSimpleName());
        }

        public void onMatch(RelOptRuleCall call) {
            RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

            final LogicalProject project = call.rel(0);
            project.explain(rw);

            final JdbcTableScan scan = call.rel(1);
            scan.explain(rw);

            RelOptTable table = scan.getTable();
            String schemaName = table.getQualifiedName().get(0);
            String tableName = table.getQualifiedName().get(1);


            List<Pair<RexNode, String>> namedProjects = project.getNamedProjects();
            logger.debug("namedProjects for " + schemaName + "." + tableName + ": " +
                    namedProjects.stream()
                            .map(n -> n.getValue())
                            .collect(Collectors.joining(", ")));
        }
    }
}