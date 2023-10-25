package nablarch.fw.reader;

import nablarch.common.databind.LineNumber;
import nablarch.common.databind.csv.Csv;

@Csv(type = Csv.CsvType.DEFAULT, properties = {"age", "name"}, headers = {"年齢", "氏名"})
public class DataBindTestForm {
    private Integer age;
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
