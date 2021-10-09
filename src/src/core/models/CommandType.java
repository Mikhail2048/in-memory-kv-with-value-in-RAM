package src.core.models;

public enum CommandType {
    GET("GET"),
    PUT("PUT");

    final String command;

    CommandType(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
