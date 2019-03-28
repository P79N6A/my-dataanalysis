package test;

import com.nicia.bocai.dataanalysis.common.CommonUtil;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.junit.Test;

import java.util.*;

public class Test2 {


    @Test
    public void test(){
        String a = "1";
        String b = new String("1");
        boolean flag = true;
        switch (b){
            case "1": System.out.println("b equals 1"); flag = false; break ;
        }

        switch (a){
            case "1": if(flag){System.out.println("a == 1");}
        }
    }
}
