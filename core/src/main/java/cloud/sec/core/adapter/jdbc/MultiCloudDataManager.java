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


    public static MultiCloudFieldSet findFields(final RelNode node) {
        logger.debug("RelVisitor:Init");

        //EXPLAIN: I need list of tables and list of projects.
        // From that I can get table fields, table schema and table name,
        // and project fields.
        // Then, for(project) {
        //          for(table) {
        //              if(table.fields.contains(project.field)) { -- match
        //                  do stuff.

        final Set<MultiCloudField<String, String, String>> usedFields = new HashSet<>();

        Map<RelOptTable, Set<String>> tables = new HashMap<>();
        Set<String> projects = new HashSet<>();

        final boolean[] isUpdate = {false};

        new RelVisitor() {
            /**
             * Starts an iteration.
             */
            public RelNode go(RelNode p) {
                try {
                    visit(p, 0, null);
                } catch (ReturnedValue e) {
                    // Rewriting cannot be performed
                    isUpdate[0] = e.value;
                }
                return p;
            }

            @Override
            public void visit(final RelNode node, final int ordinal, final RelNode parent) {
                debug(node, parent);

                if (node instanceof TableScan) {
                    tables.put(node.getTable(), getFieldNames(node));
                }

                if (node instanceof TableModify) {
                    TableModify tableModify = (TableModify) node;
                    TableModify.Operation operation = tableModify.getOperation();

                    RelOptTable table = node.getTable();
                    String schemaName = table.getQualifiedName().get(0);
                    String tableName = table.getQualifiedName().get(1);
                    Set<String> fieldNames = new HashSet<>();

                    // if it's TableModify DELETE, it does its own thing with Project and Scan, so we're ok. We can handle that.

                    //TODO: if it's TableModify INSERT, insert all table fields fields right here
                    if (operation == TableModify.Operation.INSERT) {
                        fieldNames = getFieldNames(tableModify.deriveRowType().getFieldList());
                    }

                    //TODO: if it's UPDATE, check operation=[UPDATE], updateColumnList=[[age]]
                    if (operation == TableModify.Operation.UPDATE) {
                        fieldNames = new HashSet<>(tableModify.getUpdateColumnList());
                    }

                    // TODO: handle mapping for INSERT and UPDATE here, since they're special
                    for (String field : fieldNames) {
                        usedFields.add(MultiCloudField.of(schemaName, tableName, field));
                    }

                    if (operation == TableModify.Operation.UPDATE) {
                        throw new ReturnedValue(true);
                    }
                }

                if (node instanceof Project) {
                    projects.addAll(getFieldNames(node));
                }

                super.visit(node, ordinal, parent); // visit children
            }
        }.go(node);

        // EXPLAIN: can we map (most of) them now? Yes, we can!
        if (!isUpdate[0]) { // this is a part about "most of them"
            for (String projectFieldName : projects) {
                tables.forEach((table, fields) -> {
                    if (fields.contains(projectFieldName)) {
                        String schemaName = table.getQualifiedName().get(0);
                        String tableName = table.getQualifiedName().get(1);
                        usedFields.add(MultiCloudField.of(schemaName, tableName, projectFieldName));
                    }
                });
            }
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
        return getFieldNames(getFields(node));
    }

    private static Set<String> getFieldNames(List<RelDataTypeField> fields) {
        HashSet<String> names = new HashSet<>();

        fields.forEach(field -> names.add(field.getName()));

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
