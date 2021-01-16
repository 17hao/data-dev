package xyz.shiqihao.phoenix;

import java.sql.*;

public class Test1 {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = DriverManager.getConnection("jdbc:phoenix:shiqihao.xyz");
        PreparedStatement stmt = connection.prepareStatement("select * from test_phoenix_table");
        ResultSet rset = stmt.executeQuery();
        while (rset.next()) {
            System.out.println(rset.getString("VAL"));
        }
        stmt.close();
        connection.close();
    }
}
