package mapreduce;

import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
        // Add files to input map
        // getFiles calls mapWork once finished
        ReadFiles.getFiles();

    }

}
