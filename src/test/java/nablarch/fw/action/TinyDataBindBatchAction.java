package nablarch.fw.action;

import nablarch.core.validation.ee.ValidatorUtil;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.reader.DataBindTestForm;

public class TinyDataBindBatchAction extends DataBindBatchAction<DataBindTestForm>{

    @Override
    public Class<DataBindTestForm> getInputDataType() {
        return DataBindTestForm.class;
    }

    @Override
    public String getDataFileName() {
        return "test";
    }

    public Result handle(DataBindTestForm form, ExecutionContext ctx) {
        ValidatorUtil.validate(form);
        return new Result.Success(form.getName());
    }
}
