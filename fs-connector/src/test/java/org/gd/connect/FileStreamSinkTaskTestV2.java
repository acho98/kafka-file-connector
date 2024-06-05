package org.gd.connect;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FileStreamSinkTaskTestV2 {
    private static final String FILE_PREFIX = "test";
    private static final String TEST_TOPIC = "test-topic";

    private FileStreamSinkTask task;
    private File outputDir;
    private Clock clock;

    @BeforeEach
    public void setup() throws IOException {
        outputDir = Files.createTempDirectory("kafka-connect-sink-tests").toFile();
        clock = Clock.systemUTC();
        task = new FileStreamSinkTask();
        task.setClock(clock);
    }

    @AfterEach
    public void teardown() {
        task.stop();
        for (File file : outputDir.listFiles()) {
            boolean isDeleted = file.delete();
            if (isDeleted) {
                System.out.println("File deleted: " + file.getPath());
            } else {
                System.out.println("Failed to delete file: " + file.getPath());
            }
        }
        outputDir.delete();
    }

    private Map<String, String> basicProps() {
        Map<String, String> props = new HashMap<>();
        props.put(FileStreamSinkTask.FILE_CONFIG, new File(outputDir, FILE_PREFIX).getAbsolutePath());
        return props;
    }

    @Test
    void testPut() throws IOException {
        Map<String, String> props = basicProps();
        task.start(props);

        SinkRecord record1 = new SinkRecord(TEST_TOPIC, 0, null, null, null, "line1", 0);
        SinkRecord record2 = new SinkRecord(TEST_TOPIC, 0, null, null, null, "line2", 1);
        task.put(Arrays.asList(record1, record2));

        File[] outputFiles = outputDir.listFiles();
        assertEquals(1, outputFiles.length);

        String fileContent = new String(Files.readAllBytes(outputFiles[0].toPath()), StandardCharsets.UTF_8);
        assertTrue(fileContent.contains("line1"));
        assertTrue(fileContent.contains("line2"));

        for (File file : outputFiles) {
            System.out.println("File path: " + file.getPath());
            System.out.println("File name: " + file.getName());
        }
    }

    @Test
    void testFileRolloverBasedOnSize() throws IOException {
        Map<String, String> props = basicProps();
        props.put(FileStreamSinkTask.MAX_SIZE_CONFIG, "5");
        task.start(props);

        SinkRecord record1 = new SinkRecord(TEST_TOPIC, 0, null, null, null, "line1", 0);
        SinkRecord record2 = new SinkRecord(TEST_TOPIC, 0, null, null, null, "line2", 1);
        task.put(Arrays.asList(record1, record2));

        File[] outputFiles = outputDir.listFiles();
        assertEquals(2, outputFiles.length);

        for (File file : outputFiles) {
            System.out.println("File path: " + file.getPath());
            System.out.println("File name: " + file.getName());
        }
    }

    @Test
    void testFileRolloverBasedOnDate() throws IOException {
        Map<String, String> props = basicProps();
        task.start(props);

        SinkRecord record1 = new SinkRecord(TEST_TOPIC, 0, null, null, null, "line1", 0);
        task.put(Arrays.asList(record1));

        // Advance the clock by one day
        clock = Clock.fixed(Instant.now().plusSeconds(60 * 60 * 24), ZoneId.systemDefault());
        task.setClock(clock);

        SinkRecord record2 = new SinkRecord(TEST_TOPIC, 0, null, null, null, "line2", 1);
        task.put(Arrays.asList(record2));

        File[] outputFiles = outputDir.listFiles();
        assertEquals(2, outputFiles.length);

        for (File file : outputFiles) {
            System.out.println("File path: " + file.getPath());
            System.out.println("File name: " + file.getName());
        }
    }
}
