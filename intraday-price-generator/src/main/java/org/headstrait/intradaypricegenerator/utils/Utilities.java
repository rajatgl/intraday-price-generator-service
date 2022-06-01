package org.headstrait.intradaypricegenerator.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class Utilities {

    private Utilities() {
    }

    /**
     * this method loads the properties mentioned in the file path.
     *
     * @param filePath of the file to load properties from.
     * @return properties to be utilizes by our service.
     */
    public static Properties loadProperties(String filePath){
        final Properties envProps = new Properties();
        try(FileInputStream input = new FileInputStream(filePath)) {
            envProps.load(input);
        }catch(FileNotFoundException exception){
            log.error("Config file NOT FOUND. Specify correct path.");
        }catch (IOException exception){
            log.error(exception.getMessage());
        }
        return envProps;
    }

    /**
     * used to handle multithreading.
     *
     * @param task future task to be awaited for completion.
     */
    public static void await(Future<?> task) {
        try {
            if (task.isDone())
                task.get();
        }catch (Exception exception){
            exception.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * used to handle multithreading.
     *
     * @param tasks list of future tasks to be awaited for completion.
     */
    public static void await(List<Future<?>> tasks) {
        tasks.forEach(task -> {
            try {
                if(task.isDone())
                    task.get();
            } catch (Exception exception) {
                exception.printStackTrace();
                Thread.currentThread().interrupt();
            }
        });
    }



}
