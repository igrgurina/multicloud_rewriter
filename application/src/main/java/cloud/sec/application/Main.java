package cloud.sec.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.sql.*;
import java.util.Properties;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] argv) {
        //String query = "INSERT INTO `employees` (`id`, `age`,`first`,`last`) VALUES (9, 20, 'ime', 'prezime')";
        //String query = "SELECT `age` FROM employees WHERE `age` < 30";
        String query = "SELECT * FROM employees"; // WHERE `id` IN (100,2,3,4)";
        String dbSettingsFile = "application/src/main/resources/calcite.properties";

        runQuery(query, dbSettingsFile);
    }

    private static void runQuery(String query, String dbSettingsFile) {
        try {
            FileReader dbSettingsReader = new FileReader(dbSettingsFile);
            Properties dbSettings = new Properties();
            dbSettings.load(dbSettingsReader);

            Class.forName(dbSettings.getProperty("driver"));
            Connection connection = DriverManager.getConnection(dbSettings.getProperty("url"), dbSettings);

            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query);

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
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
