package nablarch.fw.action;

import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReaderFactory;
import nablarch.fw.Handler;
import nablarch.fw.Result;
import nablarch.fw.reader.ValidatableDataBindRecordReader;

@Published
public abstract class DataBindBatchActionBase<D>
        extends BatchActionBase<D>
        implements Handler<D, Result>, DataReaderFactory<D> {

    /**
     * 入力データの型を返す。
     *
     * @return 入力データの型
     */
    public abstract Class<D> getInputDataType();

    /**
     * 入力ファイルのファイル名を返す。
     *
     * @return 入力ファイルのファイル名
     * @see nablarch.core.util.FilePathSetting
     */
    public abstract String getDataFileName();

    /**
     * 入力ファイル配置先の論理名を返す。
     * <p/>
     * デフォルト実装では"input"を返す。
     * デフォルトの入力ファイル配置先以外から入力ファイルを取得する場合は、
     * このメソッドをオーバーライドする。
     *
     * @return 入力ファイル配置先の論理名
     */
    public String getDataFileDirName() {
        return "input";
    }

    /**
     * 入力ファイルのバリデーションを実装したオブジェクトを返す。
     * <p/>
     * デフォルト実装ではバリデーションは行われない。
     * 入力ファイルのバリデーションが必要な場合にオーバーライドすること。
     *
     * @return {@code null}または入力ファイルのバリデーションを実装したオブジェクト
     */
    public ValidatableDataBindRecordReader.DataBindFileValidatorAction<D> getValidatorAction() {
        return null;
    }


}
