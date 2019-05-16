package cloud.sec.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.sql.*;
import java.util.Properties;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] argv) {
        String scanProjectAll = "SELECT * FROM employees";
        String scanProjectOne = "SELECT `age` FROM employees";
        String scanFilterProjectAll = "SELECT * FROM employees WHERE `id` IN (100,2,3,4)";
        String scanFilterProjectOne = "SELECT `age` FROM employees WHERE `age` < 30";
        String tableModify = "INSERT INTO `employees` (`id`, `age`,`first`,`last`) VALUES (9, 20, 'ime', 'prezime')";

        String dbSettingsFile = "application/src/main/resources/calcite.properties";

        String query = tableModify;

        execute(query, dbSettingsFile);
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
