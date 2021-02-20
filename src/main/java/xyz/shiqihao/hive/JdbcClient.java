package xyz.shiqihao.hive;

import java.sql.*;

public class JdbcClient {
    private static final String TABLE_NAME = "employees";

    private static final String JDBC_URL = "jdbc:hive2://aliyun-ecs:10000/default";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection connection = DriverManager.getConnection(JDBC_URL);
        PreparedStatement statement = connection.prepareStatement("select * from " + TABLE_NAME);
        statement.setString(1, TABLE_NAME);
        ResultSet res = statement.executeQuery();
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" +
                    res.getString(3) + "\t" + res.getString(4));
        }
    }
}
