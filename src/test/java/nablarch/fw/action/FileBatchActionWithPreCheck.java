package nablarch.fw.action;

import nablarch.core.dataformat.DataRecord;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.reader.ValidatableFileDataReader.FileValidatorAction;

public class FileBatchActionWithPreCheck extends FileBatchAction {
    @Override
    public String getDataFileName() {
        return "test.dat";
    }
    @Override
    public String getFormatFileName() {
        return "test";
    }
    
    @Override
    public FileValidatorAction getValidatorAction()  {
        return new FileValidatorAction() {
            /** ヘッダーレコード呼び出しの度にコールバックされる。  */
            public Result doHeader(DataRecord record, ExecutionContext ctx) {
                return new Result.Success(record.getString("type"));
            }
            /** データレコード呼び出しの度にコールバックされる。  */
            public Result doData(DataRecord record, ExecutionContext ctx) {
                return new Result.Success(record.getString("type"));
            }
            /** トレーラレコード呼び出しの度にコールバックされる。 */
            public Result doTrailer(DataRecord record, ExecutionContext ctx) {
                return new Result.Success(record.getString("type"));
            }
            /** ファイル終端に達するとコールバックされる。*/
            public void onFileEnd(ExecutionContext ctx) {
            }
            /** ファイル読み込みの最中にエラーが発生するとコールバックされる。 */
            public void onFileError(Throwable e, ExecutionContext ctx) {
            }
        };
    }

    public Result doHeader(DataRecord record, ExecutionContext ctx) {
        return new Result.Success(record.getString("type"));
    }
    
    public Result doData(DataRecord record, ExecutionContext ctx) {
        return new Result.Success(record.getString("type"));
    }
    
    public Result doTrailer(DataRecord record, ExecutionContext ctx) {
        return new Result.Success(record.getString("type"));
    }
}
