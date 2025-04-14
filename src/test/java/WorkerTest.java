import org.example.*;
import org.example.model.KeyValue;
import org.example.model.Task;
import org.example.model.TypeTask;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkerTest {

    @Mock
    private Coordinator coordinator;

    private Worker worker;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        worker = new Worker(coordinator);
    }

    @Test
    void testMap() {
        Worker worker = new Worker(null);
        String content = "test test test";
        List<KeyValue> result = worker.map(content);

        assertEquals(3, result.size());
        assertEquals("test", result.get(0).getKey());
        assertEquals("1", result.get(0).getValue());
    }

    @Test
    void testReduce() {
        Worker worker = new Worker(null);
        List<String> values = List.of("1", "1", "1", "1");
        String result = worker.reduce(values);

        assertEquals("4", result);
    }

    @Test
    void testRunWithMapTask() {
        Task mapTask = new Task(1, "test.txt", TypeTask.MAP);
        when(coordinator.getTask()).thenReturn(mapTask, null);
        when(coordinator.getCountReduceTask()).thenReturn(2);

        worker.run();
        verify(coordinator, times(2)).getTask();
        verify(coordinator, times(1)).getCountReduceTask();
    }

    @Test
    void testRunWithReduceTask() {
        Task reduceTask = new Task(1," ", TypeTask.REDUCE);
        when(coordinator.getTask()).thenReturn(reduceTask, null);
        when(coordinator.getCountMapTask()).thenReturn(1);

        worker.run();

        verify(coordinator, times(2)).getTask();
        verify(coordinator, times(1)).getCountMapTask();
    }
}