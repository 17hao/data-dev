package xyz.shiqihao.phoenix;

import java.sql.*;
import java.time.Instant;
import java.util.UUID;

/**
 * my_pk varchar not null primary key, my_col varchar
 */
public class CRUD {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = DriverManager.getConnection("jdbc:phoenix:39.99.43.51");
        insert(connection);
        get(connection);
        update(connection);
        delete(connection);
        connection.commit();
        connection.close();
    }

    static void insert(Connection connection) {
        try {
            connection.createStatement().executeUpdate("upsert into test_phoenix_table values('pk-1', 'value-1')");
            PreparedStatement stmt = connection.prepareStatement("upsert into test_phoenix_table values(?, ?)");
            stmt.setString(1, UUID.randomUUID().toString());
            stmt.setString(2, Instant.now().toString());
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void get(Connection connection) {
        try {
            PreparedStatement stmt = connection.prepareStatement("select * from test_phoenix_table");
            ResultSet rset = stmt.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("my_pk") + " = " + rset.getString("my_col"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void update(Connection connection) {
        try {
            PreparedStatement stmt = connection.prepareStatement("upsert into test_phoenix_table values('pk-1', ?)");
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void delete(Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute("delete from test_phoenix_table where my_pk = 'pk_1'");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
