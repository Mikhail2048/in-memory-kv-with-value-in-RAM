package src;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InputParser {

    public CommandType extractCommand(String rawInput) {
        if (rawInput.split(" ")[0].equals(CommandType.GET.getCommand())) {
            return CommandType.GET;
        }
        return CommandType.PUT;
    }

    public List<Record> parsePut(String dataOnlyInput) {
        final String[] recordsAsStringArray = dataOnlyInput.split(";");
        return Arrays.stream(recordsAsStringArray)
                .map(s -> s.split(","))
                .map(values -> new Record(values[0].trim(), values[1].trim()))
                .collect(Collectors.toList());
    }

    public String parseGet(String dataOnlyInput) {
        return dataOnlyInput.trim();
    }
}
