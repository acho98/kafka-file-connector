// FileStreamSinkConnector.java

package org.gd.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStreamSinkConnector extends SinkConnector {
    public static final String FILE_CONFIG = "file";
    public static final String MAX_SIZE_CONFIG = "max.size";
    public static final long DEFAULT_MAX_SIZE = 10; // This is just an example, you can adjust this value as needed

    private static final Logger log = LogManager.getLogger(FileStreamSinkConnector.class); // change here

    private String filename;
    private long maxSize;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
        maxSize = Long.parseLong(props.getOrDefault(MAX_SIZE_CONFIG, String.valueOf(DEFAULT_MAX_SIZE)));

        // add exception handling here
        try {
            // whatever your logic here
        } catch (Exception e) {
            log.error("Failed to start FileStreamSinkConnector due to error: ", e);
            throw new RuntimeException("Failed to start FileStreamSinkConnector due to error: ", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (filename != null)
                config.put(FILE_CONFIG, filename);
            config.put(MAX_SIZE_CONFIG, String.valueOf(maxSize));
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FILE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Destination filename for the output")
                .define(MAX_SIZE_CONFIG, ConfigDef.Type.LONG, DEFAULT_MAX_SIZE, ConfigDef.Importance.MEDIUM, "Maximum size of output file");
    }
}
