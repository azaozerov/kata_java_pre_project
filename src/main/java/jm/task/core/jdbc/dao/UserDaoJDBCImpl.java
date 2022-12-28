package jm.task.core.jdbc.dao;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import jm.task.core.jdbc.util.*;
import jm.task.core.jdbc.model.*;


public class UserDaoJDBCImpl implements UserDao {
    private Connection connection = null;
    private Boolean usersExist = null;
    private String usersTableName = "my_users";
    private final String createTableSQL = MessageFormat.format(
            """
                    CREATE TABLE {0} (
                        id          bigint  NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        name        varchar(100) NOT NULL,
                        last_name   varchar(100) NOT NULL,
                        age         tinyint NOT NULL)
                  """, usersTableName);
    private final String dropTableSQL = MessageFormat.format("DROP TABLE {0}", usersTableName);
    private final String insertUserSQL = MessageFormat.format("INSERT INTO {0}(name, last_name, age) VALUES ({1})", usersTableName, "\"{0}\", \"{1}\", {2}") ;
    private final String deleteUserSQL = MessageFormat.format("DELETE FROM {0} WHERE id = {1}", usersTableName, "{0}");
    private final String getAllUsersSQL = MessageFormat.format("SELECT * FROM {0}", usersTableName);
    private final String clearTableSQL = MessageFormat.format("TRUNCATE TABLE {0}", usersTableName);

    public UserDaoJDBCImpl() {
        try {
            connection = Util.getMySQLConnection();
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void createUsersTable() {
        try (Statement stmt = connection.createStatement()) {
            if (checkTableExists(usersTableName) == false) {
                stmt.execute(createTableSQL);
                usersExist = true;
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void dropUsersTable() {
        try (Statement stmt = connection.createStatement()) {
            if (checkTableExists(usersTableName)) {
                stmt.execute(dropTableSQL);
                usersExist = false;
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Statement stmt = connection.createStatement()) {
            if (checkTableExists(usersTableName)) {
                stmt.executeUpdate(MessageFormat.format(insertUserSQL, name, lastName, age));
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public void removeUserById(long id) {
        try (Statement stmt = connection.createStatement()) {
            if (checkTableExists(usersTableName)) {
                stmt.executeUpdate(MessageFormat.format(deleteUserSQL, id));
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    public List<User> getAllUsers() {
        List<User> usersList = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(getAllUsersSQL)) {
            if (checkTableExists(usersTableName)) {
                ResultSet result = stmt.executeQuery();
                while (result.next()) {
                    User u = new User(result.getString("name"), result.getString("last_name"), result.getByte("age"));
                    u.setId(result.getLong("id"));
                    usersList.add(u);
                }
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
            return null;
        }
        return usersList;
    }

    public void cleanUsersTable() {
        try (Statement stmt = connection.createStatement()) {
            if (checkTableExists(usersTableName)) {
                stmt.execute(clearTableSQL);
            }
        } catch (SQLException sqlException) {
            errorOut(sqlException);
        }
    }

    private boolean checkTableExists(String tableName) throws SQLException  {
        if (usersExist == null) {
            Statement stmt = connection.createStatement();
            ResultSet result = stmt.executeQuery(MessageFormat.format("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE table_name = \"{0}\"", usersTableName));
            usersExist = result.next();
        }
        return usersExist.booleanValue();
    }

    private void errorOut(SQLException e) {
        System.out.println("Connection error:");
        e.printStackTrace();
    }
}
