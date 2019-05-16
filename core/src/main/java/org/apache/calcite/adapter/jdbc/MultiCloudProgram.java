package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

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
}

