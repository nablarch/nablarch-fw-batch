package nablarch.fw.action;

import nablarch.core.validation.ee.ValidatorUtil;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.reader.DataBindTestForm;
import nablarch.fw.reader.ValidatableDataBindRecordReader.DataBindFileValidatorAction;


public class TinyDataBindBatchActionWithPreCheck extends DataBindBatchAction<DataBindTestForm>{

    @Override
    public Class<DataBindTestForm> getInputDataType() {
        return DataBindTestForm.class;
    }

    @Override
    public String getDataFileName() {
        return "test";
    }

    @Override
    public DataBindFileValidatorAction<DataBindTestForm> getValidatorAction() {

        return new DataBindFileValidatorAction<>() {
            @Override
            public void onFileEnd(ExecutionContext ctx) {
                // nop
            }

            @Override
            public Object handle(DataBindTestForm dataBindTestForm, ExecutionContext context) {
                ValidatorUtil.validate(dataBindTestForm);
                return new Result.Success();
            }
        };
    }

    public Result handle(DataBindTestForm form, ExecutionContext ctx) {
        ValidatorUtil.validate(form);
        return new Result.Success(form.getName());
    }
}
