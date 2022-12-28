package jm.task.core.jdbc.dao;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import jm.task.core.jdbc.util.*;
import jm.task.core.jdbc.model.*;


public class UserDaoJDBCImpl implements UserDao {
    private String usersTableName = "my_users";

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        String createTableSQL = MessageFormat.format(
                """
                        CREATE TABLE IF NOT EXISTS {0} (
                            id          bigint  NOT NULL AUTO_INCREMENT PRIMARY KEY,
                            name        varchar(100) NOT NULL,
                            last_name   varchar(100) NOT NULL,
                            age         tinyint NOT NULL)
                      """, usersTableName);
        try (Connection connection = Util.getMySQLConnection();
            Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void dropUsersTable() {
        String dropTableSQL = MessageFormat.format("DROP TABLE IF EXISTS {0}", usersTableName);
        try (Connection connection = Util.getMySQLConnection();
            Statement stmt = connection.createStatement()) {
            stmt.execute(dropTableSQL);
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String insertUserSQL = MessageFormat.format("INSERT INTO {0}(name, last_name, age) VALUES (\"{1}\", \"{2}\", {3})", usersTableName, name, lastName, age) ;
        try (Connection connection = Util.getMySQLConnection();
            PreparedStatement stmt = connection.prepareStatement(insertUserSQL)) {
            stmt.executeUpdate(insertUserSQL);
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void removeUserById(long id) {
        String deleteUserSQL = MessageFormat.format("DELETE FROM {0} WHERE id = {1}", usersTableName, id);
        try (Connection connection = Util.getMySQLConnection();
            PreparedStatement stmt = connection.prepareStatement(deleteUserSQL)) {
            stmt.executeUpdate(MessageFormat.format(deleteUserSQL, id));
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public List<User> getAllUsers() {
        String getAllUsersSQL = MessageFormat.format("SELECT * FROM {0}", usersTableName);
        List<User> usersList = new ArrayList<>();
        try (Connection connection = Util.getMySQLConnection();
             PreparedStatement stmt = connection.prepareStatement(getAllUsersSQL)) {
            ResultSet result = stmt.executeQuery();
            while (result.next()) {
                User u = new User(result.getString("name"), result.getString("last_name"), result.getByte("age"));
                u.setId(result.getLong("id"));
                usersList.add(u);
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
            return null;
        }
        return usersList;
    }

    public void cleanUsersTable() {
        String clearTableSQL = MessageFormat.format("TRUNCATE TABLE {0}", usersTableName);
        try (Connection connection = Util.getMySQLConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute(clearTableSQL);
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }


    private void errorOut(SQLException e) {
        System.out.println("Connection error:");
        e.printStackTrace();
    }
}
