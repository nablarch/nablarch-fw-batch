package nablarch.fw.action;

import java.math.BigDecimal;

import nablarch.core.dataformat.DataRecord;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;

/**
 * データ部の読み込み中に例外をスローするAction。
 * @author Masato Inoue
 */
public class TinyFileBatchAction2 extends TinyFileBatchAction {

    // エラー値
    public static int errorValue = 0;
    
    public Result doData(DataRecord record, ExecutionContext ctx) {
        if(errorValue == 0) { throw new IllegalStateException("エラーとする値を設定してください。");};

        if(new BigDecimal(errorValue).equals(record.get("amount"))) {
            throw new RuntimeException("error");
        }
        return new Result.Success(record.getString("type"));
    }
}