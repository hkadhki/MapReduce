package org.example;

import lombok.Data;
import org.example.model.Task;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Класс-координатор для управления выполнением map-reduce задач.
 */
@Data
public class Coordinator {
    private final Integer countMapTask;
    private final Integer countReduceTask;
    private final Integer countWorker;
    private ExecutorService workersPool;
    private final ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<>();
    private AtomicInteger count = new AtomicInteger(0);
    CyclicBarrier barrier;

    /**
     * Конструктор координатора.
     *
     * @param countMapTask количество map-задач
     * @param countReduceTask количество reduce-задач
     * @param countWorker количество рабочих потоков
     */
    public Coordinator(Integer countMapTask, Integer countReduceTask, Integer countWorker) {
        this.countMapTask = countMapTask;
        this.countReduceTask = countReduceTask;
        this.countWorker = countWorker;
        this.workersPool = Executors.newFixedThreadPool(countWorker);
        this.barrier = new CyclicBarrier(countWorker);
    }

    /**
     * Запускает выполнение задач в пуле рабочих потоков.
     */
    public void execute(){
        for(int i=0; i<countWorker; i++){
            workersPool.execute(new Worker(this));
        }
    }

    /**
     * Добавляет новую задачу в очередь на выполнение.
     *
     * @param task задача для добавления
     */
    public void addTask(Task task) {
        tasks.add(task);
    }

    /**
     * Получает следующую задачу из очереди.
     *
     * @return следующая задача или null, если задачи закончились
     */
    public Task getTask(){
        try {
            if (count.get() == countMapTask) {
                barrier.await();
            }
        }catch (InterruptedException | BrokenBarrierException e) {
            System.out.println(e.getMessage());
        }
        count.getAndIncrement();
        return tasks.poll();
    }
}
