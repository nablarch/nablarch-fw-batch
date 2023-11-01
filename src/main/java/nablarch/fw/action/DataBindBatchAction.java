package nablarch.fw.action;

import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReaderFactory;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.Result;
import nablarch.fw.reader.DataBindRecordReader;
import nablarch.fw.reader.ResumeDataReader;
import nablarch.fw.reader.ValidatableDataBindRecordReader;

public abstract class DataBindBatchAction<D>
        extends DataBindBatchActionBase<D>
        implements Handler<D, Result>, DataReaderFactory<D> {
    /**
     * FileBatchActionオブジェクトを生成する。
     */
    @Published
    public DataBindBatchAction() {
        super();
    }

    // -------------------------------------------------- no need to override
    /**
     * データリーダを作成する。
     * <p/>
     * この実装では、入力ファイルを読み込む{@link DataBindRecordReader}を作成し、
     * {@link ResumeDataReader}にラップして返却する。<br/>
     * また、入力ファイルの事前検証処理が必要な場合は{@link #getValidatorAction()}をオーバーライドし、
     * FileRecordReaderを{@link ValidatableDataBindRecordReader}でラップする。
     */
    public ResumeDataReader<D> createReader(ExecutionContext context) {
        ValidatableDataBindRecordReader.DataBindFileValidatorAction<D> validator = getValidatorAction();
        DataBindRecordReader<D> reader = (validator == null)
                ? new DataBindRecordReader<>(getInputDataType())
                : new ValidatableDataBindRecordReader<>(getInputDataType())
                .setValidatorAction(validator);

        reader.setDataFile(getDataFileDirName(), getDataFileName());

        return new ResumeDataReader<D>().setSourceReader(reader);
    }
}
