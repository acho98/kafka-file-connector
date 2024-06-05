package org.gd.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.time.Clock;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class FileStreamSinkTask extends SinkTask {
    private String filename;
    private String filenamePrefix;
    private long maxSize;
    private long currentSize;
    private PrintWriter writer;
    protected static final String FILE_CONFIG = "file";
    protected static final String MAX_SIZE_CONFIG = "max.size";
    private static final Logger logger = LogManager.getLogger(FileStreamSinkTask.class);

    private Clock clock = Clock.systemDefaultZone(); // Default to the system clock

    private final Object lock = new Object();

    public String getFilename() {
        synchronized (lock) {
            return filename;
        }
    }

    @Override
    public String version() {
        return new FileStreamSinkConnector().version();
    }

    public PrintWriter getWriter() {
        synchronized (lock) {
            return this.writer;
        }
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void start(Map<String, String> props) {
        filenamePrefix = props.get(FILE_CONFIG);
        synchronized (lock) {
            filename = generateFilename();
        }

        String maxSizeStr = props.get(MAX_SIZE_CONFIG);
        if (maxSizeStr != null) {
            try {
                maxSize = Long.parseLong(maxSizeStr);
            } catch (NumberFormatException e) {
                logger.error("Invalid format for MAX_SIZE_CONFIG");
                throw new ConnectException("Invalid format for MAX_SIZE_CONFIG", e);
            }
        } else {
            maxSize = 1024; // Default value
        }
        currentSize = 0;

        try {
            this.writer = createPrintWriter(filename);
        } catch (IOException e) {
            logger.error("Failed to open the PrintWriter", e);
            throw new ConnectException("Failed to open the PrintWriter", e);
        }
    }

    protected PrintWriter createPrintWriter(String filename) throws IOException {
        return new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        synchronized (lock) {
            // Get the current date
            String currentDate = new SimpleDateFormat("yyyyMMdd").format(Date.from(clock.instant()));

            // Check if the current date is not in the filename, if not then rollover
            if (!filename.contains(currentDate)) {
                rollover();
            }

            for (SinkRecord record : records) {
                String logEntry = String.format("Topic: %s, Key: %s, Value: %s", record.topic(), record.key(), record.value());
                if (currentSize + logEntry.length() > maxSize) {
                    rollover();
                }
                try {
                    writer.println(logEntry);
                    currentSize += logEntry.length();
                } catch (RuntimeException e) {
                    logger.error("Failed to write record to file", e);
                    rollover();
                }
            }
            writer.flush();
        }
    }

    private void rollover() {
        writer.close();
        filename = generateFilename();

        try {
            writer = createPrintWriter(filename);
        } catch (IOException e) {
            logger.error("File rollover failed", e);
            throw new ConnectException("File rollover failed", e);
        }

        currentSize = 0;
    }

    private String generateFilename() {
        String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmssSSS").format(Date.from(clock.instant()));
        return String.format("%s-%s.log", filenamePrefix, timestamp);
    }

    @Override
    public void stop() {
        logger.info("Stopping FileStreamSinkTask");
        writer.close();
    }
}
