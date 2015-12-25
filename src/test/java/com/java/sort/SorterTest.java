package com.java.sort;

import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(EasyMockRunner.class)
public class SorterTest {
    @Test
    public void testMerge() {
        List<String> list1 = new ArrayList();
        List<String> list2 = new ArrayList();
        list2.add("aaa");
       list2.add("bbb");
        list2.add("ccc");

        list1.add("kkk");
        list1.add("lll");
        list1.add("mmm");
        List<String> list3 = new ArrayList();
        list3.add("xxx");
        list3.add("yyy");
        list3.add("zzz");
        List<String> buffer = new ArrayList<>();
        List<List<String>> sources = new ArrayList<>();
        sources.add(list1);
        sources.add(list2);
        sources.add(list3);
        for (List<String> list : sources) {
            String head = list.get(0);
            buffer.add(head);
        }
        List<String> output = new ArrayList<>();

        while (!buffer.isEmpty()) {
            String smallest = buffer.get(0);
            int minidx = 0;
            for (int i = 0; i < buffer.size(); i++) {
                if (buffer.get(i).compareTo(smallest) <= 0) {
                    smallest = buffer.get(i);
                    minidx = i;
                }
            }
            List<String> br = sources.get(minidx);
            String current = br.remove(0);
            output.add(current);
            if (!smallest.equals(current)) {
                buffer.set(minidx, current);
            } else if (!br.isEmpty()) {
                buffer.set(minidx, br.get(0));
            } else {
                sources.remove(minidx);
                buffer.remove(minidx);
            }

        }
        System.out.println(output);

    }
}