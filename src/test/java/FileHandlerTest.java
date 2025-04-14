import org.example.FileHandler;
import org.example.model.KeyValue;
import org.junit.jupiter.api.*;


import java.io.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
class FileHandlerTest {

    private static final String FILE = "test.txt";

    @BeforeEach
    void setUp() throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE))) {
            writer.write("test test\ntest test\ntest test");
        }
    }

    @AfterEach
    void tearDown() {
        new File(FILE).delete();
        new File("result.txt").delete();
        Arrays.stream(Objects.requireNonNull(new File(".").listFiles()))
                .filter(f -> f.getName().startsWith("mr-"))
                .forEach(File::delete);
    }

    @Test
    void testReadMapFile() {
        String content = FileHandler.readMapFile(FILE);
        assertEquals("test test test test test test", content);
    }

    @Test
    void testWriteMapFile() {
        HashMap<Integer, List<KeyValue>> map = new HashMap<>();
        map.put(0, List.of(new KeyValue("key1", "value1"), new KeyValue("key2", "value2")));
        map.put(1, List.of(new KeyValue("key3", "value3")));

        FileHandler.writeMapFile(1, map);

        assertTrue(new File("mr-1-0.txt").exists());
        assertTrue(new File("mr-1-1.txt").exists());
    }

    @Test
    void testReadReduceFile() throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("mr-0-1.txt"))) {
            writer.write("key1 value1\nkey2 value2");
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("mr-1-1.txt"))) {
            writer.write("key3 value3");
        }

        List<KeyValue> result = FileHandler.readReduceFile(1, 2);
        assertEquals(3, result.size());
    }

    @Test
    void testWriteReduceFile() {
        FileHandler.writeReduceFile("testKey", "42");

        assertTrue(new File("result.txt").exists());
    }
}