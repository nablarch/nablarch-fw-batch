package nablarch.fw.reader;

import nablarch.common.databind.LineNumber;
import nablarch.common.databind.csv.Csv;
import nablarch.core.validation.ee.NumberRange;
import nablarch.core.validation.ee.SystemChar;

@Csv(type = Csv.CsvType.DEFAULT, properties = {"age", "name"}, headers = {"年齢", "氏名"})
public class DataBindTestForm {

    @NumberRange(min = 0, max = 150, message = "年齢が不正です。")
    private Integer age;

    @SystemChar(charsetDef = "全角文字", message = "氏名は全角文字で入力してください。")
    private String name;

    private long lineNumber;

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @LineNumber
    public long getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(long lineNumber) {
        this.lineNumber = lineNumber;
    }

}
