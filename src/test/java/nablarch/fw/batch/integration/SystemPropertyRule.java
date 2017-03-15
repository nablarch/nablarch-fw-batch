package nablarch.fw.batch.integration;

import org.junit.rules.ExternalResource;

import java.util.HashMap;
import java.util.Map;

public class SystemPropertyRule extends ExternalResource {

    private Map<String, String> original;

    @Override
    protected void before() throws Throwable {
        super.before();
        original = new HashMap<String, String>();
    }

    public void setSystemProperty(String key, String value) {
        original.put(key, System.getProperty(key));
        System.setProperty(key, value);
    }

    @Override
    protected void after() {
        for (Map.Entry<String, String> entry : original.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        }
    }

}
