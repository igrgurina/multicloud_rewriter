package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;

public class MultiCloudJdbcSchema extends JdbcSchema {
    private static final Logger logger = LoggerFactory.getLogger(MultiCloudJdbcSchema.class);

    /**
     * Creates a MultiCloud JDBC schema.
     *
     * @param dataSource Data source
     * @param dialect    SQL dialect
     * @param convention Calling convention
     * @param catalog    Catalog name, or null
     * @param schema     Schema name pattern
     */
    public MultiCloudJdbcSchema(DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String catalog, String schema) {
        super(dataSource, dialect, convention, catalog, schema);
        logger.debug("CREATED NEW MULTI CLOUD SCHEMA");
    }

    /**
     * Transforms an original {@link JdbcSchema} into {@link MultiCloudJdbcSchema} with custom {@link MultiCloudConvention} calling convention that inserts custom rules from
     * {@link MultiCloudRuleManager}.
     *
     * @param originalSchema Original Jdbc schema created from .json configuration factory
     */
    private static MultiCloudJdbcSchema createFrom(JdbcSchema originalSchema) {
        logger.debug("CreateFrom TRANSFORMING " + originalSchema.toString() + " TO NEW SCHEMA WITH CUSTOM CONVENTION THAT ADDS OUR OWN RULES");
        return new MultiCloudJdbcSchema(
                originalSchema.dataSource,
                originalSchema.dialect,
                new MultiCloudConvention(originalSchema.convention.dialect, originalSchema.convention.expression, originalSchema.convention.getName()), // adds OUR OWN RULES
                originalSchema.catalog,
                originalSchema.schema);
    }

    // Factory used by .json configuration
    public static class Factory implements SchemaFactory {
        public static final Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            logger.debug("INIT SCHEMA FROM JSON CONFIGURATION: " + name);
            JdbcSchema original = MultiCloudJdbcSchema.create(parentSchema, name, operand);

            for(String s : parentSchema.getSubSchemaNames()){
                logger.debug("SubSchema: " + s);

            }

            MultiCloudJdbcSchema target = createFrom(original);
            return target;
        }
    }
}

