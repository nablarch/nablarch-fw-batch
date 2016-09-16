package nablarch.fw.action;

import nablarch.core.dataformat.DataRecord;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReaderFactory;
import nablarch.fw.ExecutionContext;
import nablarch.fw.reader.FileDataReader;
import nablarch.fw.reader.ResumeDataReader;
import nablarch.fw.reader.ValidatableFileDataReader;
import nablarch.fw.reader.ValidatableFileDataReader.FileValidatorAction;

/**
 * ファイル入力バッチにおける業務処理のサポートクラス。
 * <p/>
 * 業務処理を実装するメソッドのシグニチャは{@link nablarch.fw.handler.RecordTypeBinding}の仕様に従って作成すること。<br/>
 * {@code
 * do[レコードタイプ名](DataRecord record, ExecutionContext context)
 * }
 *
 * @see nablarch.fw.reader.FileDataReader
 * @see nablarch.fw.handler.RecordTypeBinding
 * @author Iwauo Tajima
 */
public abstract class FileBatchAction
    extends    FileBatchActionBase<DataRecord>
    implements DataReaderFactory<DataRecord> {

    /**
     * FileBatchActionオブジェクトを生成する。
     */
    @Published
    public FileBatchAction() {
        super();
    }

    // -------------------------------------------------- no need to override
    /**
     * データリーダを作成する。
     * <p/>
     * この実装では、入力ファイルを読み込む{@link nablarch.core.dataformat.FileRecordReader}を作成し、
     * {@link ResumeDataReader}にラップして返却する。<br/>
     * また、入力ファイルの事前検証処理が必要な場合は{@link #getValidatorAction()}をオーバーライドし、
     * FileRecordReaderを{@link ValidatableFileDataReader}でラップする。
     */
    public ResumeDataReader<DataRecord> createReader(ExecutionContext context) {
        FileValidatorAction validator = getValidatorAction();
        FileDataReader reader = (validator == null)
                                ? new FileDataReader()
                                : new ValidatableFileDataReader()
                                     .setValidatorAction(validator);
                                
        reader.setDataFile(getDataFileDirName(), getDataFileName())
                .setLayoutFile(getFormatFileDirName(), getFormatFileName());

        return new ResumeDataReader<DataRecord>().setSourceReader(reader);
    }
}
