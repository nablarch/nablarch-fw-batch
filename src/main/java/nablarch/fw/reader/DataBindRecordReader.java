package nablarch.fw.reader;

import nablarch.common.databind.ObjectMapper;
import nablarch.common.databind.ObjectMapperFactory;
import nablarch.core.util.FilePathSetting;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.reader.iterator.ObjectMapperIterator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * ファイルデータを１レコードづつ読み込み、
 * 読み込んだフィールドの内容を{@code T}型のクラスにマッピングして返却するデータリーダ。
 * <p/>
 * 実際のレコード読み込み処理は、{@link ObjectMapper}に委譲する。
 * <p/>
 * このクラスを使用するにあたって設定が必須となるプロパティの実装例を下記に示す。
 * 以下では、マッピング先のクラスを{@code SampleForm}としている。
 * <pre>{@code
 *     DataBindRecordReader<SampleForm> reader = new DataBindRecordReader<>(SampleForm.class)
 *         //データファイルベースパス論理名とデータファイル名(拡張子無し)を設定する。
 *         .setDataFile("input", "dataFile");
 * }</pre>
 *
 * このクラスは読み込み対象のファイルが存在しない場合には例外を送出する。
 * 読み込み対象のファイルが空(0バイト)の場合は、例外の送出は行わない。
 *
 * @param <T>
 * @author Takayuki Uchida
 */
@Published(tag = "architect")
public class DataBindRecordReader<T> implements DataReader<T> {

    /** データファイル名 */
    private String dataFileName = null;

    /** データファイルのベースパス論理名 */
    private String dataFileBasePathName = null;

    /** 入力データの型 */
    private final Class<T> inputDataType;

    /** ファイル読み込み時にバッファを使用するかどうか。 */
    private boolean useBuffer = true;

    /** ファイル読み込みの際に使用するバッファのサイズ（デフォルトは8192B） */
    private int bufferSize = 8192;

    /** オブジェクトマッパーイテレータ。 */
    private ObjectMapperIterator<T> iterator;

    /**
     * コンストラクタ。
     *
     * @param inputDataType 入力データの型
     */
    public DataBindRecordReader(Class<T> inputDataType) {
        this.inputDataType = inputDataType;
    }

    @Override
    public T read(ExecutionContext executionContext) {
        if (iterator == null) {
            iterator = createObjectMapperIterator();
        }
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public boolean hasNext(ExecutionContext executionContext) {
        if (iterator == null) {
            iterator = createObjectMapperIterator();
        }
        return iterator.hasNext();
    }

    @Override
    public void close(ExecutionContext executionContext) {
        if(iterator == null) {
            return;
        }
        iterator.close();
    }

    /**
     * データファイルのファイル名を設定する。
     * <p/>
     * "input"という論理名のベースパス配下に存在する当該ファイルがデータファイルとして使用される。
     *
     * @param fileName データファイル名
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public DataBindRecordReader<T> setDataFile(String fileName) {
        return setDataFile("input", fileName);
    }

    /**
     * データファイルのベースパス論理名およびファイル名を設定する。
     * <p/>
     * 設定したベースパス配下に存在する当該のファイルがデータファイルとして使用される。
     *
     * @param basePathName ベースパス論理名
     * @param fileName データファイル名
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public DataBindRecordReader<T> setDataFile(String basePathName, String fileName) {
        this.dataFileBasePathName = basePathName;
        this.dataFileName = fileName;
        return this;
    }

    /**
     * ファイル読み込み時にバッファーを使用するか否かを設定する。
     *
     * @param useBuffer バッファーを使用する場合は {@code true}
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public DataBindRecordReader<T> setUseBuffer(boolean useBuffer) {
        this.useBuffer = useBuffer;
        return this;
    }

    /**
     * ファイル読み込み時のバッファーサイズを設定する。
     *
     * @param bufferSize バッファーサイズ
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public DataBindRecordReader<T> setBufferSize(int bufferSize) {
        if(bufferSize <= 0) {
            throw new IllegalArgumentException("buffer size was invalid. buffer size must be greater than 0.");
        }
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * {@link ObjectMapperIterator}を設定する。
     * <p>
     * すでに設定されている場合は、まずクローズしてから設定する。
     *
     * @param objectMapperIterator オブジェクトマッパーイテレータ
     */
    protected void setObjectMapperIterator(ObjectMapperIterator<T> objectMapperIterator) {
        if (null != iterator) {
            iterator.close();
        }
        iterator = objectMapperIterator;
    }

    /**
     * オブジェクトマッパーイテレータを生成する。
     *
     * @return {@link ObjectMapperIterator}オブジェクト
     * @throws IllegalStateException 必須であるプロパティが設定されていない場合、読み込み対象のファイルが存在しない場合
     */
    private ObjectMapperIterator<T> createObjectMapperIterator() {
        try {
            InputStream is = new FileInputStream(getDataFile());
            if (useBuffer) {
                return new ObjectMapperIterator<>(ObjectMapperFactory.create(inputDataType, new BufferedInputStream(is, bufferSize)));
            }
            return new ObjectMapperIterator<>(ObjectMapperFactory.create(inputDataType, is));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * データファイルオブジェクトを取得する。
     *
     * @return {@link File} データファイルオブジェクト
     */
    private File getDataFile() {
        if (StringUtil.isNullOrEmpty(dataFileName)) {
            throw new IllegalStateException("data file name was blank. data file name must not be blank.");
        }
        if (StringUtil.isNullOrEmpty(dataFileBasePathName)) {
            throw new IllegalStateException("data file base path name was blank. data file base path name must not be blank.");
        }

        FilePathSetting filePathSetting = FilePathSetting.getInstance();
        return filePathSetting.getFileWithoutCreate(dataFileBasePathName, dataFileName);
    }
}
