// FileStreamSinkTaskTest.java

package org.gd.connect;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileStreamSinkTaskTest {
    private static final String FILE_CONFIG = "file";
    private static final String MAX_SIZE_CONFIG = "max.size";

    private FileStreamSinkTask task;
    private Map<String, String> props;
    private String filenamePrefix = "test-output.log";

    @BeforeEach
    public void setup() {
        task = new FileStreamSinkTask();
        props = new HashMap<>();
        props.put(FILE_CONFIG, filenamePrefix);
        props.put(MAX_SIZE_CONFIG, "1024");
        task.start(props);

        File file = new File(task.getFilename());
        System.out.println("File absolute path: " + file.getAbsolutePath());
    }

    @AfterEach
    public void cleanup() throws IOException {
        // This line is commented so the file won't be deleted after the tests, as per your request
        // Files.deleteIfExists(Paths.get(task.getFilename()));
    }

    @Test
    public void testPutRecordInFile() throws IOException {
        // Prepare a dummy record
        SinkRecord record = new SinkRecord(
                "topic", 0, null, "key", null, "value", 0
        );

        // Put record in the file
        task.put(Collections.singleton(record));

        // Assert that the file exists
        File file = new File(task.getFilename());
        assertTrue(file.exists());

        // Read the file
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();

        // Assert that the record is correctly written in the file
        assertEquals("Topic: topic, Key: key, Value: value", line);

        reader.close();
    }
}
