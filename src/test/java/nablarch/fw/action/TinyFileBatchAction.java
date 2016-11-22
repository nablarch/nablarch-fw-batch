package nablarch.fw.action;

import nablarch.core.dataformat.DataRecord;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;

public class TinyFileBatchAction extends FileBatchAction {
    @Override
    public String getDataFileName() {
        return "test";
    }
    @Override
    public String getFormatFileName() {
        return "test";
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