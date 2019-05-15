package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.List;

/**
 * The most typical program is an invocation of the volcano planner with a
 * particular {@link org.apache.calcite.tools.RuleSet}
 */
public class MultiCloudProgram implements Program {

    //~ Hook features ---------------------------------------------

    @Override
    public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
        Program hep = Programs.hep(MultiCloudRuleManager.rules(null), false, null);

        RelNode run = hep.run(planner, rel, requiredOutputTraits, materializations, lattices);

        return run;
    }
}
