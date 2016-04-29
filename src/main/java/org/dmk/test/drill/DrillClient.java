package org.dmk.test.drill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 *
 */
public class DrillClient {

    private static Logger logger = LoggerFactory.getLogger(DrillClient.class);

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.drill.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:drill:drillbit=localhost");
        Statement st = connection.createStatement();

        logger.info("select from csvh file");
        ResultSet rs = st.executeQuery("select * from dfs" +
                ".`/media/data/dev/projects/test/spark-test/src/test/resources/fruits.csvh`");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        logger.info("select from parquet file");
        rs = st.executeQuery("select * from dfs" +
                ".`/media/data/dev/projects/test/spark-test/target/fruit-cube.parquet`");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

}
