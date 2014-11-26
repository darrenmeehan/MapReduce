package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapReduce {
    /*
     * Called by ReadFiles.getFiles()
     * Description:
     */
    public static void mapWork() throws IOException {

        System.out.println("Now running MapReduce.mapWork()");
        ReadFiles.input.put("file1.txt", "foo foo bar cat dog dog");

        // Output HashMap containing 
        final Map<String, Map<String, Integer>> output = new HashMap<>();

        // MAP:
        final List<MappedItem> mappedItems = new LinkedList<>();

        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        List<Thread> mapCluster = new ArrayList<>(ReadFiles.input.size());

        Iterator<Map.Entry<String, String>> inputIter = ReadFiles.input.entrySet().iterator();
        while (inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            final String file = entry.getKey();
            final String contents = entry.getValue();

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    Functions.map(file, contents, mapCallback);
                }
            });
            mapCluster.add(t);
            t.start();
        }

        // wait for mapping phase to be over:
        for (Thread t : mapCluster) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // GROUP:
        Map<String, List<String>> groupedItems = new HashMap<>();

        Iterator<MappedItem> mappedIter = mappedItems.iterator();
        while (mappedIter.hasNext()) {
            MappedItem item = mappedIter.next();
            String word = item.getWord();
            String file = item.getFile();
            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<>();
                groupedItems.put(word, list);
            }
            list.add(file);
        }

        // REDUCE:
        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        List<Thread> reduceCluster = new ArrayList<>(groupedItems.size());

        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while (groupedIter.hasNext()) {
            Map.Entry<String, List<String>> entry = groupedIter.next();
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    Functions.reduce(word, list, reduceCallback);
                }
            });
            reduceCluster.add(t);
            t.start();
        }

        // wait for reducing phase to be over:
        for (Thread t : reduceCluster) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println(output);
    }

}
