package xyz.shiqihao.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Creation {
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String jdbcUrl = "jdbc:phoenix:aliyun";
        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement stmt = connection.createStatement();
        System.out.println(stmt.execute("create test-phoenix-table(pk PRIMARY KEY VARCHAR, col-1 VARCHAR);"));
    }
}
