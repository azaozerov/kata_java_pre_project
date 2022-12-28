package jm.task.core.jdbc;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();
        userService.createUsersTable();
        userService.saveUser("Sergey", "Sergeev", (byte) 5);
        userService.saveUser("Alex", "Alexandrov", (byte) 31);
        userService.saveUser("Semen", "Semenov", (byte) 24);
        userService.saveUser("Pavel", "Pavlov", (byte) 55);
        userService.getAllUsers().stream().forEach( (u) -> System.out.println(u.toString()));
        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
