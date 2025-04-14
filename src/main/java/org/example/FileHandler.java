package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.model.KeyValue;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Утилитный класс для работы с файлами map-reduce.
 */
@Slf4j
public class FileHandler {

    /**
     * Читает содержимое файла и возвращает его в виде одной строки.
     *
     * @param fileName имя файла для чтения
     * @return строка, содержащая всё содержимое файла
     */
    public static String readMapFile(String fileName) {
        log.info("Reading file: {}", fileName);
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            while (true){
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                builder.append(line).append(" ");
            }
            log.info("Successfully read file: {}", fileName);
        } catch (IOException e) {
            log.warn("Error reading file: {}", fileName);//TODO
        }
        return builder.toString().trim();
    }

    /**
     * Записывает промежуточные результаты map-фазы в несколько файлов.
     * Имена файлов формируются по шаблону "mr-{ID_map_задачи}-{ID_reduce_задачи}.txt".
     *
     * @param taskMapId идентификатор map-задачи
     * @param map данные для записи в формате HashMap<Integer, List<KeyValue>>
     */
    public static void writeMapFile(Integer taskMapId, HashMap<Integer, List<KeyValue>> map) {
        log.info("Writing reduce files for task ID: {}", taskMapId);
        map.forEach((id, keyValues) -> {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("mr-" + taskMapId + "-" + id + ".txt"))) {
                for (KeyValue keyValue : keyValues) {
                    writer.write(keyValue.getKey() + " " + keyValue.getValue());
                    writer.newLine();
                }
                log.info("Successfully wrote temp file: mr-{}-{}.txt", taskMapId, id);
            } catch (IOException e) {
                log.warn("Error writing to temp file: mr-{}-{}.txt", taskMapId, id);//TODO
            }
        });
    }

    /**
     * Читает все промежуточные файлы, относящиеся к конкретной reduce-задаче.
     *
     * @param id идентификатор reduce-задачи
     * @param countMapTask общее количество map-задач
     * @return список {@link KeyValue}, собранных из всех соответствующих файлов
     */
    public static List<KeyValue> readReduceFile(Integer id, Integer countMapTask){
        List<KeyValue> list = new ArrayList<>();
        for (int i = 0; i < countMapTask; i++) {
            try (BufferedReader reader = new BufferedReader(new FileReader("mr-" + i + "-" + id + ".txt"))) {
                log.info("Reading reduce file: mr-{}-{}.txt", i, id);
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] arr = line.split(" ");
                    if (arr.length == 2) {
                        list.add(new KeyValue(arr[0], arr[1]));
                    }
                }
            } catch (IOException e) {
                log.warn("Error reduce temp file: mr-{}-{}.txt", i, id);//TODO
            }
        }

        return list;
    }

    private static final Lock lock = new ReentrantLock();

    /**
     * Потокобезопасная запись финального результата reduce-фазы в файл result.txt.
     *
     * @param key ключ для записи
     * @param result результат для записи
     */
    public static void writeReduceFile(String key, String result) {
        log.info("Writing result to file ");
        lock.lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("result.txt", true))) {
            writer.write(key + " " + result);
            writer.newLine();
        } catch (IOException e) {
            log.warn("Error writing result to file ");//TODO
        } finally {
            lock.unlock();
        }
    }
}
