import org.example.Coordinator;
import org.example.model.Task;
import org.example.model.TypeTask;
import org.example.Worker;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CoordinatorTest {

    private Coordinator coordinator;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        executorService = mock(ExecutorService.class);
        coordinator = new Coordinator(2, 2, 2);
        coordinator.setWorkersPool(executorService);
    }

    @Test
    void testAddTask() {
        Task task = new Task(1, "file.txt", TypeTask.MAP);
        coordinator.addTask(task);

        assertEquals(1, coordinator.getTasks().size());
    }

    @Test
    void testGetTask() {
        Task task = new Task(1,  "file.txt", TypeTask.MAP);
        coordinator.addTask(task);

        Task result = coordinator.getTask();
        assertEquals(1, result.getId());
    }

    @Test
    void testExecute() {
        coordinator.execute();
        verify(executorService, times(2)).execute(any(Worker.class));
    }

    @Test
    void testBarrierSync() throws Exception {
        Coordinator testCoordinator = new Coordinator(1, 1, 1);
        Task task = new Task(1, "file.txt", TypeTask.MAP);
        testCoordinator.addTask(task);

        assertNotNull(testCoordinator.getTask());
        assertNull(testCoordinator.getTask());
    }
}