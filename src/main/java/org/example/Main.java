package org.example;

import org.example.model.Task;
import org.example.model.TypeTask;

import java.util.concurrent.ExecutorService;

public class Main {
    public static void main(String[] args) {
        Coordinator coordinator = new Coordinator(4,2,4);
        coordinator.addTask(new Task(0, "0.txt", TypeTask.MAP));
        coordinator.addTask(new Task(1, "1.txt", TypeTask.MAP));
        coordinator.addTask(new Task(2, "2.txt", TypeTask.MAP));
        coordinator.addTask(new Task(3, "3.txt", TypeTask.MAP));
        coordinator.addTask(new Task(0, null, TypeTask.REDUCE));
        coordinator.addTask(new Task(1, null, TypeTask.REDUCE));
        coordinator.execute();

        ExecutorService workersPool = coordinator.getWorkersPool();
        workersPool.shutdown();

    }
}