package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.model.KeyValue;
import org.example.model.Task;
import org.example.model.TypeTask;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Класс-исполнитель для выполнения map-reduce задач.
 */
@Slf4j
public class Worker extends Thread {
    private final Coordinator coordinator;

    /**
     * Конструктор потока.
     *
     * @param coordinator координатор, распределяющий задачи
     */
    public Worker(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    /**
     * Основной метод выполнения потока.
     */
    public void run() {

        log.info("Worker started.");
        Task task;
        while ((task = coordinator.getTask()) != null) {
            if(task.getTypeTask() == TypeTask.MAP){
                log.info("Starting Map task for file: {} (Task ID: {})", task.getFilename(), task.getId());
                String content = FileHandler.readMapFile(task.getFilename());
                List<KeyValue> keyValues = map(content);


                // Создаем HashMap под каждый reduce файл
                HashMap<Integer, List<KeyValue>> mapFiles = new HashMap<>();
                keyValues.forEach(keyValue -> {
                    int fileId = keyValue.getKey().hashCode() % coordinator.getCountReduceTask();
                    mapFiles.computeIfAbsent(fileId, k -> new ArrayList<>()).add(keyValue);
                });
                //Сохраняем значения
                FileHandler.writeMapFile(task.getId(), mapFiles);
                log.info("Map task for file: {} (Task ID: {}) completed.", task.getFilename(), task.getId());
            }else{
                log.info("Starting Reduce task for ID: {}", task.getId());
                List<KeyValue> keyValues = FileHandler.readReduceFile(task.getId(), coordinator.getCountMapTask());
                keyValues.sort(Comparator.comparing(KeyValue::getKey));

                // Создаем HashMap под каждый ключ
                Map<String, List<String>> mapFiles = new HashMap<>();
                keyValues.forEach(keyValue -> {
                    mapFiles.computeIfAbsent(keyValue.getKey(), k -> new ArrayList<>()).add(keyValue.getValue());
                });

                //Сохраняем значения
                mapFiles.forEach((key, value) -> {
                    FileHandler.writeReduceFile(
                            key,
                            reduce(value)
                    );
                });
                log.info("Reduce task for ID: {} completed.", task.getId());
            }
        }
        log.info("No more tasks. Worker is shutting down.");
    }

    /**
     * Функция map - преобразует входной текст в список пар (ключ, значение).
     *
     * @param content входной текст для обработки
     * @return список пар {@link KeyValue}
     */
    public List<KeyValue> map(String content) {
        String[] words = content.split("\\W+");
        return Stream.of(words).map(s -> new KeyValue(s, "1")).collect(Collectors.toList());
    }

    /**
     * Функция reduce - обрабатывает значения.
     *
     * @param values список значений для одного ключа
     * @return результат в виде строки
     */
    public String reduce(List<String> values) {
        return String.valueOf(values.size());
    }


}
