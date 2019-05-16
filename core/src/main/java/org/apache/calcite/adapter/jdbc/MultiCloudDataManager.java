package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MultiCloudDataManager {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudProgram.class);

    public static Set<MultiCloudField<String, String, String>> findFields(final RelNode node) {
        final Set<MultiCloudField<String, String, String>> usedFields = Sets.newLinkedHashSet();

        RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(final RelNode node, final int ordinal, final RelNode parent) {
                if (node instanceof TableScan && parent instanceof Project) {

                    final TableScan scan = (TableScan) node;
                    //scan.explain(rw);

                    RelOptTable table = scan.getTable();
                    String schemaName = table.getQualifiedName().get(0);
                    String tableName = table.getQualifiedName().get(1);

                    // FIXME: this doesn't work for PROJECT->FILTER->SCAN type queries that are very common
                    final Project project = (Project) parent;
                    //project.explain(rw);

                    List<Pair<RexNode, String>> namedProjects = project.getNamedProjects();

                    for (Pair<RexNode, String> field : namedProjects) {
                        String fieldName = field.getValue();
                        usedFields.add(MultiCloudField.of(schemaName, tableName, fieldName));
                    }
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(node);

        logger.debug("\n\t" + usedFields.stream()
                .map(n -> n.toString())
                .collect(Collectors.joining("\n\t")));
        return usedFields; //usedTables;
    }
}
