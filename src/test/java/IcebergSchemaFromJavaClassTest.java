import org.apache.iceberg.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class IcebergSchemaFromJavaClassTest {

    class Class1 {
        private int a;
        private String b;
        private List<Integer> c;
        private Class2 d;
        private List<Class2> e;
        private Map<String, Class2> f;

        public int getA() {
            return a;
        }

        public void setA(int a) {
            this.a = a;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }

        public List<Integer> getC() {
            return c;
        }

        public void setC(List<Integer> c) {
            this.c = c;
        }

        public Class2 getD() {
            return d;
        }

        public void setD(Class2 d) {
            this.d = d;
        }

        public List<Class2> getE() {
            return e;
        }

        public void setE(List<Class2> e) {
            this.e = e;
        }

        public Map<String, Class2> getF() {
            return f;
        }

        public void setF(Map<String, Class2> f) {
            this.f = f;
        }
    }
    class Class2 {
        private String g;
        private List<Class3> h;

        public String getG() {
            return g;
        }

        public void setG(String g) {
            this.g = g;
        }
    }
    class Class3 {
        private String i;

        public String getI() {
            return i;
        }

        public void setI(String i) {
            this.i = i;
        }
    }
    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void generateSchema() throws Exception {
        Schema schema = IcebergSchemaFromJavaClass.generateSchema(Class1.class);
        System.out.println(schema);

    }
}