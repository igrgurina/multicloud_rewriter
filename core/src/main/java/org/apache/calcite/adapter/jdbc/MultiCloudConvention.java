package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.sql.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiCloudConvention extends JdbcConvention {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudConvention.class);

    /** Cost of a MultiCloud node versus implementing an equivalent node in a "typical"
     * calling convention. */
    public static final double COST_MULTIPLIER = 0.8d;

    public MultiCloudConvention(SqlDialect dialect, Expression expression, String name) {
        super(dialect, expression, name);
    }

    @Override
    public void register(RelOptPlanner planner) {
        super.register(planner);

        // add my custom rule
        logger.debug("ADDING CUSTOM RULES TO CONVENTION");
        for (RelOptRule rule : MultiCloudRuleManager.rules(this)) {
            planner.addRule(rule); // planner.addRule(MultiCloudProjectRewriterRule.INSTANCE);
        }
    }
}