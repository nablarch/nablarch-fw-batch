package nablarch.fw.reader.iterator;

import nablarch.common.databind.ObjectMapperFactory;
import nablarch.fw.TestSupport;
import nablarch.fw.reader.DataBindTestForm;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * 本クラスでは、{@link ObjectMapperIterator}のメソッドのうち、他クラスのテストで動作確認できていないものをテストする。
 */
@SuppressWarnings("NonAsciiCharacters")
public class ObjectMapperIteratorTest {

    private final File dataFile = new File(System.getProperty("java.io.tmpdir"), "record.csv");

    private ObjectMapperIterator<DataBindTestForm> sut = null;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @After
    public void tearDown() throws Exception {
        if (sut != null) {
            sut.close();
        }

        if (dataFile.exists()) {
            dataFile.delete();
        }
    }

    @Test
    public void クローズ済みの場合_次のデータはnullになること() throws Exception {
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8);

        sut = new ObjectMapperIterator<>(ObjectMapperFactory.create(DataBindTestForm.class, new FileInputStream(dataFile)));

        sut.close();
        assertNull(sut.next());
    }

    @Test
    public void removeメソッドはサポートされていないこと() throws Exception {
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8);

        sut = new ObjectMapperIterator<>(ObjectMapperFactory.create(DataBindTestForm.class, new FileInputStream(dataFile)));

        assertThrows(UnsupportedOperationException.class, () -> sut.remove());
    }

}