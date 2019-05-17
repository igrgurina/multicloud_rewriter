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

    private static final Logger logger = LoggerFactory.getLogger(MultiCloudProgram.class);

    @Override
    public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
        // TODO: remove this after testing; this is at the moment used for comparison to check if relVisitor behaves in the same way so that we can replace this with RelVisitor if it does.
        RelNode data = Programs.hep(Collections.singleton(MultiCloudRuleManager.MultiCloudDataCollector.INSTANCE), false, null)
                .run(planner, rel, requiredOutputTraits, materializations, lattices);

        // TODO: make use of this
        // EXPLAIN: run standard program to get "optimized" query on original database, since then we can guarantee some stuff, like filter into join and stuff.
        // MultiCloudDataManager.findFields(Programs.standard().run(planner, data, requiredOutputTraits, materializations, lattices));
        MultiCloudDataManager.findFields(data);

        Program hep = Programs.hep(MultiCloudRuleManager.rules(), false, null);

        RelNode run = hep.run(planner, data, requiredOutputTraits, materializations, lattices);

        return run;
    }
}

