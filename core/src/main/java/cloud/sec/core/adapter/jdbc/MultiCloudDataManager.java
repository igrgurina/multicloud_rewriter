package cloud.sec.core.adapter.jdbc;

import cloud.sec.core.tools.MultiCloudFieldSet;
import cloud.sec.core.tools.MultiCloudFieldSets;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ControlFlowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class MultiCloudDataManager {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudDataManager.class);
    private static final RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

    public static Set<RelOptTable> findTables(final RelNode node) {
        final Set<RelOptTable> tables = Sets.newLinkedHashSet();

        // tableVisitor
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan || node instanceof TableModify) {
                    tables.add(node.getTable());
                }
                super.visit(node, ordinal, parent);
            }
        }.go(node);

        return tables;
    }

    // FIXME: make this return something...
    // TODO: maybe I don't even need this.
    public static void handle(Project project) {
        new RelVisitor() {

            @Override
            // EXPLAIN: initial input is a child of pNode = project node. go through the tree and discover first scan, that should be the correct one
            public void visit(RelNode pNode, int ordinal, RelNode parent) {
                if (pNode instanceof TableScan) {
                    // EXPLAIN: if it's a Table Scan, this is most likely the correct one

                    final TableScan scan = (TableScan) pNode;
                    // EXPLAIN: get information about table
                    RelOptTable table = scan.getTable();
                    String schemaName = table.getQualifiedName().get(0);
                    String tableName = table.getQualifiedName().get(1);

                    // TODO: add check that Project->Fields are subset of TableScan->Fields to confirm this is a correct TableScan - in case of Join, this is more complicated
                    for (RelDataTypeField relDataTypeField : table.getRowType().getFieldList()) {
                        relDataTypeField.getName();
                    }

                    // EXPLAIN: go through the list of fields in Project and add them to the usedFields, together with discovered TableScan schema and table names.
//                    for (Pair<RexNode, String> field : namedProjects) {
//                        String fieldName = field.getValue();
//                        usedFields.add(MultiCloudField.of(schemaName, tableName, fieldName));
//                    }
                } else if (pNode instanceof Join) {
                    // EXPLAIN: if it's a Join, we need to start new relVisitors on each branch
                    // TODO: add relVisitors on each child branch of JOIN
                    Join join = (Join) pNode;
                    if (join.getInputs().size() != 2) {
                        // Bail out
                        throw new ReturnedValue(false);
                    }

                    // FIXME: ADD REAL HANDLERS FOR JOIN
                    // TODO: join handler should look for tableScan, another join or project
                    // First branch should have the query (with write ID filter conditions)
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, RelNode parent) {
                            if (node instanceof TableScan ||
                                    node instanceof Filter ||
                                    node instanceof Project ||
                                    node instanceof Join) {
                                // We can continue
                                super.visit(node, ordinal, parent);
                            } else if (node instanceof Aggregate) {
                                // We can continue
                                super.visit(node, ordinal, parent);
                            } else {
                                throw new ReturnedValue(false);
                            }
                        }
                    }.go(join.getInput(0));
                    // Second branch should only have the MV
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, RelNode parent) {
                            if (node instanceof TableScan) {
                                // We can continue
                                // TODO: Need to check that this is the same MV that we are rebuilding
                                RelOptTable table = (RelOptTable) node.getTable();

                            } else if (node instanceof Project) {
                                // We can continue
                                super.visit(node, ordinal, parent);
                            }
                        }
                    }.go(join.getInput(1));
                }

                // EXPLAIN: otherwise, continue to next child
                super.visit(pNode, ordinal, parent);
            }
        }.go(project.getInput(0)); // EXPLAIN: Project always has single child
    }

    public static MultiCloudFieldSet findFields(final RelNode node) {
        logger.debug("RelVisitor:Init");

        //EXPLAIN: I need list of tables and list of projects.
        // From that I can get table fields, table schema and table name,
        // and project fields.
        // Then, for(project) {
        //          for(table) {
        //              if(table.fields.contains(project.field)) { -- match
        //                  do stuff.

        final Set<MultiCloudField<String, String, String>> usedFields = Sets.newLinkedHashSet();

        Map<RelOptTable, Set<String>> tables = new HashMap<>();
        Set<String> projects = new HashSet<>();

        new RelVisitor() {
            @Override
            public void visit(final RelNode node, final int ordinal, final RelNode parent) {
                debug(node, parent);

                if (node instanceof TableScan || node instanceof TableModify) {
                    tables.put(node.getTable(), getFieldNames(node));
                }

                if (node instanceof Project) { // EXPLAIN: if it's a Project, we want to find him a matching TableScan OR Join->TableScans to get Schema and Table information
                    projects.addAll(getFieldNames(node));

                    //EXPLAIN: lets ignore Project handler for now, maybe we can get information we need without it
                    // handle((Project) node);

                }

                super.visit(node, ordinal, parent); // visit children
            }
        }.go(node);

        // TODO: can we map them now?
        for (RelDataTypeField field : projects) {
            tables.forEach((table, fields) -> {
                if(fields.contains(field)) {
                    String schemaName = table.getQualifiedName().get(0);
                    String tableName = table.getQualifiedName().get(1);
                    usedFields.add(MultiCloudField.of(schemaName, tableName, field.getName()));
                }
            });
        }

        logger.debug("RelVisitor:Done\n\t" + usedFields.stream()
                .map(MultiCloudField::toString)
                .collect(Collectors.joining("\n\t")));

        return MultiCloudFieldSets.ofList(usedFields);
    }

    private static void debug(final RelNode node, final RelNode parent) {
        if (parent != null) {
            logger.debug("RelVisitor.Parent\t" + " : " + parent.getDigest());
        }
        logger.debug("\t\tRelVisitor.Node\t" + " : " + node.getDigest());
        for (RelNode child : node.getInputs()) {
            logger.debug("\t\t\t\tRelVisitor.Child\t" + " : " + child.getDigest());
        }
    }

    private static List<RelDataTypeField> getFields(RelNode node) {
        return node.getRowType().getFieldList();
    }

    private static Set<String> getFieldNames(RelNode node) {
        HashSet<String> names = new HashSet<>();

        getFields(node).forEach(field -> names.add(field.getName()));

        return names;
    }

    /**
     * Exception used to interrupt a visitor walk.
     */
    private static class ReturnedValue extends ControlFlowException {
        private final boolean value;

        public ReturnedValue(boolean value) {
            this.value = value;
        }
    }
}
