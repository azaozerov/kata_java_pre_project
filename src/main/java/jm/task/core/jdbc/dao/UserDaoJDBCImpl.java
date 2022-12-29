package jm.task.core.jdbc.dao;
import java.sql.*;
import java.util.*;
import jm.task.core.jdbc.util.*;
import jm.task.core.jdbc.model.*;


public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        String createTableSQL = """
                                    CREATE TABLE IF NOT EXISTS my_users (
                                        id          bigint  NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                        name        varchar(100) NOT NULL,
                                        last_name   varchar(100) NOT NULL,
                                        age         tinyint NOT NULL)
                               """;
        try (Connection connection = Util.getMySQLConnection();
            Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void dropUsersTable() {
        String dropTableSQL = "DROP TABLE IF EXISTS my_users";
        try (Connection connection = Util.getMySQLConnection();
            Statement stmt = connection.createStatement()) {
            stmt.execute(dropTableSQL);
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String insertUserSQL = "INSERT INTO my_users(name, last_name, age) VALUES (?, ?, ?)";
        try (Connection connection = Util.getMySQLConnection();
            PreparedStatement stmt = connection.prepareStatement(insertUserSQL)) {
            stmt.setString(1, name);
            stmt.setString(2, lastName);
            stmt.setByte(3, age);
            stmt.executeUpdate();
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void removeUserById(long id) {
        String deleteUserSQL = "DELETE FROM my_users WHERE id = ?";
        try (Connection connection = Util.getMySQLConnection();
            PreparedStatement stmt = connection.prepareStatement(deleteUserSQL)) {
            stmt.setLong(1, id);
            stmt.executeUpdate();
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public List<User> getAllUsers() {
        String getAllUsersSQL = "SELECT * FROM my_users";
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
        String clearTableSQL = "TRUNCATE TABLE my_users";
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
