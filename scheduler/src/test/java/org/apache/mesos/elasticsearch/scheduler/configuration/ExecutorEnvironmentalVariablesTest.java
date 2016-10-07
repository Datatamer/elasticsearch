package org.apache.mesos.elasticsearch.scheduler.configuration;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests
 */
public class ExecutorEnvironmentalVariablesTest {
    private Configuration configuration = Mockito.mock(Configuration.class);

    @Test
    public void ensureOver4GBHeapIs1024MB() throws Exception {
        int ram = 4096;
        Mockito.when(configuration.getMem()).thenReturn((double) ram);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for (Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.ES_HEAP)) {
                String val = var.getValue();
                Pattern pattern = Pattern.compile("(\\d*)m");
                Matcher matcher = pattern.matcher(val);
                assertTrue(matcher.matches());
                assertEquals(Integer.toString(ram - 1024), matcher.group(1));
            }
        }
    }

    @Test
    public void ensureUnder4GBIsLessThan256MB() throws Exception {
        int ram = 2048;
        Mockito.when(configuration.getMem()).thenReturn((double) ram);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for (Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.ES_HEAP)) {
                String val = var.getValue();
                Pattern pattern = Pattern.compile("(\\d*)m");
                Matcher matcher = pattern.matcher(val);
                assertTrue(matcher.matches());
                assertEquals(Integer.toString(ram - ram / 4), matcher.group(1));
                assertTrue(ram - Integer.parseInt(matcher.group(1)) < 1024);
                assertTrue(ram - Integer.parseInt(matcher.group(1)) < ram);
                assertTrue(ram - Integer.parseInt(matcher.group(1)) > 0);
            }
        }
    }
}
