package com.java.sort;

import com.java.sort.task.SortTask;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Created by zealot on 24.12.2015.
 */
public class Sorter {
    public static String file;
    public static String dir = "./tmp";
    private static int threads = 50;
    private static ExecutorService service;
    private static int slice = 150;
    private static final Logger log = Logger.getLogger(Sorter.class);
    private static List<BufferedReader> arrayFile = new ArrayList<>();
    private static String output = "./output";
    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption("file", true, "file to read from");
        options.addOption("dir", false, "directory to store files");
        options.addOption("threadPool", false, "number of workers");
        options.addOption("sizeOfSlice", false, "size of file parts");
        options.addOption("output", false, "output file sorted");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        file = cmd.getOptionValue("file");
        if (cmd.getOptionValue("dir") != null) {
            dir = cmd.getOptionValue("dir");
        }
        if (!dir.endsWith(File.pathSeparator))
            dir = dir + File.pathSeparator;
        if (cmd.getOptionValue("threadPool") != null) {
            threads = Integer.parseInt(cmd.getOptionValue("threadPool"));
        }
        if (cmd.getOptionValue("sizeOfSlice") != null) {
            slice = Integer.parseInt(cmd.getOptionValue("sizeOfSlice"));
        }
        if (cmd.getOptionValue("output") != null) {
            output = cmd.getOptionValue("output");
        }
        log.info("now split file to many sorted file");
        service = Executors.newFixedThreadPool(threads);
        log.info("open file " + file);
        long lineCount = Files.lines(Paths.get(file)).count();
        log.info("sort " + lineCount + " lines");
        int cut = 0;
        while (cut < lineCount) {
            cut += slice;
            SortTask task = new SortTask(cut, slice);
            service.submit(task);
        }
        service.shutdown();// wait until all task are executed
        log.info("all task successfully done, now merge file to one");
        log.info("open stream to all my file");
        Files.walkFileTree(Paths.get(dir), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                arrayFile.add(Files.newBufferedReader(file));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        log.info("stream successfully opened");
        List<String> buffer = new ArrayList<>();
        for (BufferedReader br : arrayFile) {
            String head = br.readLine();
            buffer.add(head);
        }

        while (!buffer.isEmpty()) {
            String smallest = buffer.get(0);
            int minidx = 0;
            for (int i = 0; i < buffer.size(); i++) {
                if (buffer.get(i).compareTo(smallest) >= 0) {
                    smallest = buffer.get(i);
                    minidx = i;
                }
            }
            BufferedReader br = arrayFile.get(minidx);
            String current = br.readLine();

            if (current != null) {
                Files.write(Paths.get(output), current.getBytes());
                buffer.set(minidx, current);

            } else {//buffer is empty
                arrayFile.remove(minidx);
                buffer.remove(minidx);
            }
        }

    }
}
