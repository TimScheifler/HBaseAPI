package de.hfu.infosys.models;

public class User {

    private final String userId;
    private final String user;

    public User(String userId, String user) {
        this.userId = userId;
        this.user = user;
    }

    public String getUserId() {
        return userId;
    }

    public String getUser() {
        return user;
    }

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
}

