package xyz.shiqihao.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;

public class MyUDF extends UDF {
    public MyUDF() {
        super();
    }

    protected MyUDF(UDFMethodResolver rslv) {
        super(rslv);
    }

    @Override
    public void setResolver(UDFMethodResolver rslv) {
        super.setResolver(rslv);
    }

    @Override
    public UDFMethodResolver getResolver() {
        return super.getResolver();
    }

    @Override
    public String[] getRequiredJars() {
        return super.getRequiredJars();
    }

    @Override
    public String[] getRequiredFiles() {
        return super.getRequiredFiles();
    }
}
