package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapReduce {

    public static void mapWork() throws IOException {

        // Output HasMap containing 
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
                    map(file, contents, mapCallback);
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
                    reduce(word, list, reduceCallback);
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

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for (String word : words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<>();
        list.stream().forEach((file) -> {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        });
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<>(words.length);
        for (String word : words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K, V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<>();
        list.stream().forEach((file) -> {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        });
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}
