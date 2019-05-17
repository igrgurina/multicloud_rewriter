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
        // TODO: make use of this
        MultiCloudDataManager.findFields(rel);

        return rel;
    }
}

