package com.java.sort.task;


import com.java.sort.Sorter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Created by zealot on 24.12.2015.
 */
public class SortTask implements Runnable {
    private static final Logger log = Logger.getLogger(SortTask.class);
    private int from;
    private int count;

    public SortTask(int from, int count) {
        this.from = from;
        this.count = count;
    }

    @Override
    public void run() {
        log.info("sort file " + Sorter.file + " from " + from +  " to" + (from + count));
        try (Stream<String> lines = Files.lines(Paths.get(Sorter.file))) {
            lines.skip(from);
            lines.limit(count).parallel().sorted().forEach(new Consumer<String>() {
                @Override
                public void accept(String s) {
                    try {
                        log.info("write string " + s + " to " + Paths.get(Sorter.dir , Thread.currentThread().getName(), "tempfile"));
                        Files.write(Paths.get(Sorter.dir, Thread.currentThread().getName() + "-tempfile"), s.getBytes(),
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    } catch (IOException e) {
                        log.error("error", e);
                    }
                }
            });
        } catch (IOException e) {
            log.error("error", e);
        }


    }


}
