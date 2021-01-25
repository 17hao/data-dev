package xyz.shiqihao.hive;

import java.sql.*;

public class HiveJdbcClient {
    private static final String tableName = "employees";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hive2://aliyun-ecs:10000/default");
        PreparedStatement statement = connection.prepareStatement("select * from " + tableName);
        statement.setString(1, tableName);
        ResultSet res = statement.executeQuery();
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" +
                    res.getString(3) + "\t" + res.getString(4));
        }
    }
}
