package mapreduce;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ReadFiles {

    public static Map<String, String> input = new HashMap<>();

    static String readFile(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static void getFiles() throws IOException {

        // Directory path here
        String path = "\\\\fs2\\11498402\\Desktop\\science";

        String files;
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (File listOfFile : listOfFiles) {
            if (listOfFile.isFile()) {
                files = listOfFile.getName();
                String pathToFile = listOfFile.getAbsolutePath();
                if (files.endsWith(".txt") || files.endsWith(".TXT")) {
                    String content = readFile(pathToFile, StandardCharsets.UTF_8);
                    ReadFiles.input.put(files, content);
                }
            }
        }
        System.out.println("There are " + ReadFiles.input.size() + " files.");
        MapReduce.mapWork();

    }
}
