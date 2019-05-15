package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MultiCloudDataManager {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudDataManager.class);

    public MultiCloudDataManager() {}

    public static List<RelDataTypeField> extractFieldListFromOriginalQuery(RelNode relNode) {
        List<RelDataTypeField> fieldList = new ArrayList<>();

        // CHECK: This will probably only work when we have PROJECT->SCAN type of query at the root.
        // TODO: Maybe we should add check for that pattern and search for it recursively.
        for (RelNode input : relNode.getInputs()) {
             fieldList.addAll(input.getRowType().getFieldList());
             // TODO: check if this will work with all queries

            // TODO: check if this has to be done recursively, i.e. input.getInputs()...
        }

        return fieldList;
    }

}
