package com.java.sort;

import com.java.sort.task.SortTask;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private static final Logger log = LoggerFactory.getLogger(Sorter.class);
    private static List<BufferedReader> arrayFile = new ArrayList<>();
    private static String output = "./output";
    public static void main(String[] args) throws ParseException, IOException, InterruptedException {
        Options options = new Options();
        options.addOption(Option.builder("file").hasArg().required().desc("file to read from").build());//"file", true, "file to read from");
        options.addOption(Option.builder("dir").hasArg().desc("directory to store files").build());//"dir", true, "directory to store files");
        options.addOption(Option.builder("threadpool").hasArg().desc("number of workers").build());//"threadpool", true, "number of workers");
        options.addOption(Option.builder("sizeofslice").hasArg().desc("size of file parts").build());//"sizeofslice", true, "size of file parts");
        options.addOption(Option.builder("output").hasArg().desc("path to output file").build());//"output", true, "output file sorted");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        file = cmd.getOptionValue("file");
        if (cmd.hasOption("dir")  ) {
            dir = cmd.getOptionValue("dir");
        }
        if (!dir.endsWith(File.separator))
            dir = dir + File.separator;
        Files.createDirectories(Paths.get(dir));
        if (cmd.hasOption("threadpool")  ) {
            threads = Integer.parseInt(cmd.getOptionValue("threadPool"));
        }
        if (cmd.hasOption("sizeofslice")  ) {
            log.info("size of slice " + cmd.getOptionValue("sizeofslice"));
            slice = Integer.parseInt(cmd.getOptionValue("sizeofslice"));
        }
        if (cmd.hasOption("output") ) {
            output = cmd.getOptionValue("output");
        }
        log.debug("now split file to many sorted file");
        service = Executors.newFixedThreadPool(threads);
        log.debug("open file {}", file);
        long lineCount = Files.lines(Paths.get(file)).count();
        log.debug("sort {} lines", lineCount);
        int cut = 0;
        while (cut < lineCount) {
            SortTask task = new SortTask(cut, slice);
            cut += slice;
            service.submit(task);
        }
        service.shutdown();// wait until all task are executed
        service.awaitTermination(1, TimeUnit.HOURS);
        log.debug("all task successfully done, now merge file to one");
        log.debug("open stream to all my file");
        Files.walkFileTree(Paths.get(dir), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                log.debug("add file to further-reading array {}");
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
        log.debug("stream successfully opened");
        List<String> buffer = new ArrayList<>();
        for (BufferedReader br : arrayFile) {
            String head = br.readLine();
            buffer.add(head);
        }
        while (!buffer.isEmpty()) {
            String smallest = buffer.get(0);
            int minidx = 0;
            for (int i = 0; i < buffer.size(); i++) {
                if (buffer.get(i).compareTo(smallest) <= 0) {
                    smallest = buffer.get(i);
                    minidx = i;
                }
            }
            BufferedReader br = arrayFile.get(minidx);
            String current = br.readLine();
            Files.write(Paths.get(output), (smallest + String.format("%n").intern()).getBytes() , StandardOpenOption.APPEND, StandardOpenOption.CREATE);

            if (current != null) {
                buffer.set(minidx, current);

            } else {//buffer is empty
                arrayFile.remove(minidx);
                buffer.remove(minidx);

            }
        }

    }
}
