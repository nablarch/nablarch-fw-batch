package nablarch.fw.reader;

import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.annotation.Published;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;

import java.util.LinkedList;
import java.util.List;

/**
 * ファイル内容のバリデーション機能を追加したデータリーダ。
 * <p>
 * ファイル全件の読み込みを行い、このリーダが提供する{@link DataBindFileValidatorAction}に実装されたバリデーションロジックを
 * {@link #setValidatorAction(DataBindFileValidatorAction)}から設定することができる。
 * バリデーションが正常終了した場合は、入力ファイルを開きなおして本処理を行う。
 * また、{@link #setUseCache(boolean)}に{@code true}を設定することで、バリデーション時に読み込んだデータを
 * メモリ上にキャッシュし、都度2回の読み込みを1回に削減することができる。
 * ただし、データ量によってはメモリリソースを大幅に消費する点に注意すること。
 * <p>
 * バリデーションが失敗した場合、このデータリーダは例外を送出してクローズされる。
 *
 *
 * @author Takayuki Uchida
 */
public class ValidatableDataBindRecordReader<T> extends DataBindRecordReader<T>{

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(ValidatableDataBindRecordReader.class);

    /**
     * コンストラクタ。
     *
     * @param inputDataType 入力データの型
     */
    @Published(tag = "architect")
    public ValidatableDataBindRecordReader(Class<T> inputDataType) {
        super(inputDataType);
    }

    /**
     * バリデーションを行うオブジェクトが実装するインタフェース。
     */
    @Published
    public interface DataBindFileValidatorAction<T> extends Handler<T, Object> {
        /**
         * ファイル全件の読み込みが正常終了し、
         * ファイル終端に達するとコールバックされる
         * @param ctx 実行コンテキスト
         */
        void onFileEnd(ExecutionContext ctx);
    }

    /** バリデーションを実装したハンドラ */
    private DataBindFileValidatorAction<T> validatorAction = null;

    /** バリデーション時に読み込んだデータを本処理で使用するかどうか。 */
    private boolean useCache = false;

    /** バリデーションで読み込んだデータを格納するキャッシュ */
    private List<T> recordCache = null;

    /** バリデーション済みフラグ */
    private boolean validated = false;

    /**
     * データファイルを1レコードづつ読み込む。
     * <p/>
     * ファイルがバリデーション済みでない場合、バリデーションを行う。<br/>
     * 次に読み込むレコードが存在しない場合、{@code null}を返す。
     * <p/>
     * データをキャッシュしている場合、キャッシュからデータを読み込み返却する。<br/>
     * キャッシュしていない場合、データファイルからデータを読み込み返却する。
     *
     * @param ctx 実行コンテキスト
     * @return １レコード分のデータレコード
     */
    @Override
    public T read(ExecutionContext ctx) {
        if (!validated) {
            validate(ctx);
        }
        if (useCache) {
            return (recordCache == null || recordCache.isEmpty())
                    ? null
                    : recordCache.remove(0);
        }
        return super.read(ctx);
    }

    /**
     * 次に読み込むデータが存在するかどうかを返却する。
     * <p/>
     * データをキャッシュしている場合、キャッシュから結果を返却する。<br/>
     * キャッシュしていない場合、データファイルから結果を返却する。
     *
     * @param ctx 実行コンテキスト
     * @return 次に読み込むデータが存在する場合は {@code true}
     */
    @Override
    public boolean hasNext(ExecutionContext ctx) {
        if (validated && useCache) {
            return recordCache != null && !recordCache.isEmpty();
        }
        return super.hasNext(ctx);
    }

    /**
     * このリーダの利用を停止し、内部的に保持している各種リソースを解放する。
     * <p/>
     * キャッシュを有効にしていた場合、キャッシュを削除する。
     *
     * @param ctx 実行コンテキスト
     */
    @Override
    public void close(ExecutionContext ctx) {
        if (useCache) {
            if(recordCache != null) {
                recordCache.clear();
            }
            recordCache = null;
        }
        super.close(ctx);
    }


    /**
     * バリデーションを行う。
     * <p>
     * {@link ValidatableDataBindRecordReader#useCache}が{@code true}である場合、読み込んだデータをキャッシュする。
     *
     * @param ctx 実行コンテキスト
     * @throws IllegalStateException バリデーション処理を実装したオブジェクトが{@code null}の場合
     * @throws RuntimeException 実行時例外が発生した場合
     * @throws Error エラーが発生した場合
     */
    protected void validate(ExecutionContext ctx) {
        if (validatorAction == null) {
            throw new IllegalStateException(
                    "FileValidatorAction was not set. an Object that implements the validation logic must be set."
            );
        }
        try {
            Handler<T, Object> validateHandler = validatorAction;

            if (useCache) {
                recordCache = new LinkedList<>();
            }
            while (super.hasNext(ctx)) {
                T record = super.read(ctx);
                validateHandler.handle(record, ctx);
                if (useCache) {
                    recordCache.add(record);
                }
            }
            validatorAction.onFileEnd(ctx);
            super.close(ctx);
            if (!useCache) {
                initialize();
            }
        } catch (RuntimeException | Error e) {
            try {
                close(ctx);
            } catch (Throwable t) {
                LOGGER.logWarn("failed to release file resource.", t);
            }
            throw e;
        } finally {
            validated = true;
        }
    }

    /**
     * データリーダを初期化し、再度使用できるようにする。
     */
    protected void initialize() {
        super.setObjectMapperIterator(null);
    }

    /**
     * バリデーション時に読み込んだデータをキャッシュし、本処理で使用するかどうかを設定する。
     *
     * @param useCache キャッシュを有効化する場合は{@code true}
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public ValidatableDataBindRecordReader<T> setUseCache(boolean useCache) {
        this.useCache = useCache;
        return this;
    }

    /**
     * バリデーション処理を実装したアクションクラスを設定する。
     *
     * @param validatorAction バリデーションを実装したアクションクラス
     * @return このオブジェクト自体
     * @throws IllegalArgumentException バリデーションを実装したアクションクラスが{@code null}の場合
     */
    @Published(tag = "architect")
    public ValidatableDataBindRecordReader<T> setValidatorAction(DataBindFileValidatorAction<T> validatorAction) {
        if (validatorAction == null) {
            throw new IllegalArgumentException("validator action was null. validator action must not be null.");
        }
        this.validatorAction = validatorAction;
        return this;
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
    public ValidatableDataBindRecordReader<T> setDataFile(String fileName) {
        return (ValidatableDataBindRecordReader<T>) super.setDataFile(fileName);
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
    public ValidatableDataBindRecordReader<T> setDataFile(String basePathName, String fileName) {
        return (ValidatableDataBindRecordReader<T>) super.setDataFile(basePathName, fileName);
    }

    /**
     * ファイル読み込み時にバッファーを使用するか否かを設定する。
     *
     * @param useBuffer バッファーを使用する場合は {@code true}
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public ValidatableDataBindRecordReader<T> setUseBuffer(boolean useBuffer) {
        return (ValidatableDataBindRecordReader<T>) super.setUseBuffer(useBuffer);
    }

    /**
     * ファイル読み込み時のバッファーサイズを設定する。
     *
     * @param bufferSize バッファーサイズ
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public ValidatableDataBindRecordReader<T> setBufferSize(int bufferSize) {
        return (ValidatableDataBindRecordReader<T>) super.setBufferSize(bufferSize);
    }

}
