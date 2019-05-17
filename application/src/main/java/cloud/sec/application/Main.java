package cloud.sec.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.sql.*;
import java.util.Properties;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] argv) {
        // EXPLAIN: I disabled multicloud rules since I don't need them

        String scanProjectAll = "SELECT * FROM cities";
        String scanJoinFilterProjectAll = "SELECT * FROM cities, employees WHERE employees.city_id = cities.id";
        String scanProjectOne = "SELECT `employees`.`age` FROM employees";
        String scanFilterProjectAll = "SELECT * FROM employees WHERE `id` IN (100,2,3,4)";
        String scanFilterProjectOne = "SELECT `age` FROM employees WHERE `age` < 30";
        String scanFilterProjectTwo = "SELECT `first`, `age` FROM employees WHERE `age` < 30";
        String tableModifyInsert = "INSERT INTO `employees` (`id`, `age`,`first`,`last`,`city_id`) VALUES (13, 20, 'Ivan', 'Grgurina', 1)";
        String tableModifyUpdate = "UPDATE `mc_db`.`employees` SET `id` = 13, `age` = 17, `first` = 'Ivan', `last` = 'Grgurina', `city_id` = 1 WHERE `id` = 13";
        String tableModifyUpdatePartial = "UPDATE `mc_db`.`employees` SET `age` = 25 WHERE `id` = 13";
        String tableModifyDelete = "DELETE FROM `mc_db`.`employees` WHERE `id` = 13";

        String dbSettingsFile = "application/src/main/resources/calcite.properties";

        //execute(scanProjectAll, dbSettingsFile); // works
        //execute(scanJoinFilterProjectAll, dbSettingsFile); // works
        //execute(scanProjectOne, dbSettingsFile); // works
        //execute(scanFilterProjectAll, dbSettingsFile); // works
        //execute(scanFilterProjectOne, dbSettingsFile); // works
        //execute(scanFilterProjectTwo, dbSettingsFile); // works

        execute(tableModifyInsert, dbSettingsFile); // FIXME: doesn't work because we don't handle TableModify right now
        execute(tableModifyUpdate, dbSettingsFile); // FIXME: doesn't work because we don't handle TableModify right now
        execute(tableModifyUpdatePartial, dbSettingsFile); // FIXME: doesn't work because we don't handle TableModify right now
        execute(tableModifyDelete, dbSettingsFile); // FIXME: doesn't work because we don't handle TableModify right now
    }

    private static void execute(String query, String dbSettingsFile) {
        try {
            FileReader dbSettingsReader = new FileReader(dbSettingsFile);
            Properties dbSettings = new Properties();
            dbSettings.load(dbSettingsReader);

            Class.forName(dbSettings.getProperty("driver"));
            Connection connection = DriverManager.getConnection(dbSettings.getProperty("url"), dbSettings);

            Statement statement = connection.createStatement();
            boolean isQuery = statement.execute(query);

            if (isQuery) {
                handleQuery(statement);
            } else {
                handleDML(statement);
            }

            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleQuery(Statement statement) throws Exception {
        ResultSet results = statement.getResultSet();

        ResultSetMetaData meta = results.getMetaData();
        int noOfColumns = meta.getColumnCount();
        while (results.next()) {
            for (int i = 1; i <= noOfColumns; i++) {
                String columnName = meta.getColumnName(i);
                Object columnValue = results.getObject(columnName);
                logger.info(columnName + " = " + String.format("%-15s", String.valueOf(columnValue)) + "|");
            }
        }
        results.close();
    }

    private static void handleDML(Statement statement) throws Exception {
        int result = statement.getUpdateCount();
        logger.info("RESULT: " + result + " rows affected");
    }
}
