package utils;

import java.util.*;

public class ParsedArgs {

    private final String configFile;
    private final String[] args;

    public ParsedArgs(String configFile, String[] args) {
        this.args = args;
        this.configFile = configFile;
    }

    public String getConfigFile() {
        return configFile;
    }

    public String[] getArgs() {
        return args;
    }

    public static ParsedArgs parseArgs(String[] args, String defaultConfigFile) {
        String config = defaultConfigFile;

        List<String> newArgs = new ArrayList<>();
        Collections.addAll(newArgs, args);

        Iterator<String> iter = newArgs.iterator();
        while (iter.hasNext()) {
            String param = iter.next();
            if (param.equals("-conf")) {
                if (iter.hasNext()) {
                    iter.remove();
                    config = iter.next();
                    iter.remove();
                }
                break;
            }
        }

        return new ParsedArgs(config, newArgs.toArray(new String[0]));
    }

}
