package xyz.shiqihao.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TableCreation {
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String jdbcUrl = "jdbc:phoenix:39.99.43.51";
        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement stmt = connection.createStatement();
        System.out.println(stmt.execute("create table if not exists test_phoenix_table (my_pk varchar not null primary key, my_col varchar)"));
    }
}
