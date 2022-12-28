package jm.task.core.jdbc.util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private static String url = "jdbc:mysql://localhost:3306/mysql";
    private static String user = "Artem";
    private static String password = "!QAZ2wsx";


    public static Connection getMySQLConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
