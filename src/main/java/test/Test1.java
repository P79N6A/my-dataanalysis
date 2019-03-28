package test;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.junit.Test;

public class Test1 {

    @Test
    public void test1() throws DocumentException {

        String x = "<xml><aaa>aaa</aaa><bbb>bb</bbb></xml>";
        Document d = DocumentHelper.parseText(x);
        System.out.println(d.getRootElement().element("bbb").getText());
    }
}
