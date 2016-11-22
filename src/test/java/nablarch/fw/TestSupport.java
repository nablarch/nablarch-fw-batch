package nablarch.fw;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

public class TestSupport {

    public static void createFile(File file, Charset charset, String... lines) throws Exception {
        createFile(file, System.getProperty("line.separator"), charset, lines);
    }

    public static void createFile(File file, String lf, Charset charset, String... lines) throws Exception {
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), charset));
        try {
            for (final String line : lines) {
                writer.write(line);
                writer.write(lf);
            }
        } finally {
            writer.close();
        }
    }
}
