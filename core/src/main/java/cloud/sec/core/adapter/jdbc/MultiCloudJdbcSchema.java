package cloud.sec.core.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
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

    public MultiCloudJdbcSchema(DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String catalog, String schema) {
        super(dataSource, dialect, convention, catalog, schema);
        logger.debug("CREATED NEW MULTICLOUD SCHEMA");
    }

    // Factory used by .json configuration
    public static class Factory implements SchemaFactory {
        public static final Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            logger.debug("INIT SCHEMA FROM JSON CONFIGURATION: " + name);
            JdbcSchema original = JdbcSchema.create(parentSchema, name, operand);

            MultiCloudRuleManager.MultiCloudHookManager.addHook();

            return original;
        }
    }
}

