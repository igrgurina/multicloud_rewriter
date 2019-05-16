package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The most typical program is an invocation of the volcano planner with a
 * particular {@link org.apache.calcite.tools.RuleSet}
 */
public class MultiCloudProgram implements Program {

    @Override
    public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
        // TODO: Can this be done better? Maybe RelVisitor.
        RelNode data = Programs.hep(Collections.singleton(MultiCloudRuleManager.MultiCloudDataCollector.INSTANCE), false, null)
                .run(planner, rel, requiredOutputTraits, materializations, lattices);

        Program hep = Programs.hep(MultiCloudRuleManager.rules(), false, null);

        RelNode run = hep.run(planner, data, requiredOutputTraits, materializations, lattices);

        return run;
    }

    // These following two methods are good examples of using RelVisitor that I found on the web: findTables and transformToDruidPlan
    // TODO: implement my use-case using these methods as template
    public static Set<List<String>> findTables(final RelNode node) {
        final Set<List<String>> usedTables = Sets.newLinkedHashSet();
        final RelVisitor visitor = new RelVisitor() {
            @Override public void visit(final RelNode node, final int ordinal, final RelNode parent) {
                if (node instanceof TableScan) {
                    usedTables.add(node.getTable().getQualifiedName());
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(node);
        return usedTables;
    }

    private RelNode transformToDruidPlan(RelNode root) {
        RelOptPlanner plan = root.getCluster().getPlanner();
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    final RelOptCluster cluster = node.getCluster();
                    final RelOptTable.ToRelContext context =
                            RelOptUtil.getContext(cluster);
                    final RelNode r = node.getTable().toRel(context);
                    plan.registerClass(r);
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(root);
        Program program = Programs.standard();
        RelTraitSet traits = plan.emptyTraitSet().replace(EnumerableConvention.INSTANCE);
        return program.run(plan, root, traits,
                ImmutableList.<RelOptMaterialization>of(),
                ImmutableList.<RelOptLattice>of());
    }

}

