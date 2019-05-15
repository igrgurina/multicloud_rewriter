package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MultiCloudDataManager {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudDataManager.class);

    public MultiCloudDataManager() {
    }

    public static List<RelDataTypeField> extractFieldListFromOriginalQuery(RelNode relNode) {
        List<RelDataTypeField> fieldList = new ArrayList<>();

        // handle root project - happens most often on SELECT *, SELECT 'field' and all similar queries
        if (relNode instanceof Project) {
            // CHECK this probably isn't best way to handle ROOT project
            fieldList.addAll(relNode.getRowType().getFieldList());
        }
        // keep searching inputs recursively until you find (another) Project

        // CHECK: This will probably only work when we have PROJECT->SCAN type of query at the root.
        // TODO: Maybe we should add check for that pattern and search for it recursively.
        for (RelNode input : relNode.getInputs()) {
            // find corresponding JdbcTableScan
            if (input instanceof TableScan) {
                // we can get information about table, like catalog and table names
                logger.info("qualifiedName: " + input.getTable().getQualifiedName());
            }

            fieldList.addAll(input.getRowType().getFieldList());
            // TODO: check if this will work with all queries

            // TODO: check if this has to be done recursively, i.e. input.getInputs()...
        }

        logger.info("fieldList: " + fieldList.stream()
                .map(n -> n.getName())
                .collect(Collectors.joining(", ")));

        return fieldList;
    }

    List<RelDataTypeField> fields = new ArrayList<>();

    // TODO: use RelVisitor pattern to implement this; doesn't work.
    // we have to search for Project rel to find fields, and TableScan rel to find table and schema.
    private List<RelDataTypeField> getFieldListRecursive(RelNode relNode) {
        if (relNode.getInputs().size() == 0) { // getInputs returns empty list, not null, when no inputs
            if (relNode instanceof TableScan) { // tableScan is most likely leaf with no inputs
                // I don't want TableScan fields
                // but I use TableScan to extract information about schema and table
                logger.info("qualifiedName: " + relNode.getTable().getQualifiedName());
            }
            // we're done parsing the tree
            // CHECK: wait, is this visitor pattern I'm doing here? :D hahha I should just use that... but lets see how this goes first.
            return fields;
        }
        fields.addAll(relNode.getRowType().getFieldList());

        for (RelNode input : relNode.getInputs()) {
            return getFieldListRecursive(input);
        }

        return fields;
    }
}
