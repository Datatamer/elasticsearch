package org.apache.mesos.elasticsearch.scheduler.configuration;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests
 */
public class ExecutorEnvironmentalVariablesTest {
    private final Configuration configuration = Mockito.mock(Configuration.class);

    @Test
    public void ensureOver1GBHeapIs256MB() {
        int ram = 2048;
        int heap = -1;
        Mockito.when(configuration.getMem()).thenReturn((double) ram);
        Mockito.when(configuration.getHeap()).thenReturn((double) heap);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for (Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.ES_JAVA_OPTS)) {
                final Consumer<Matcher> testCondition = matcher -> {
                    assertTrue(matcher.matches());
                    assertEquals(Integer.toString(ram - 256), matcher.group(1));
                };

                assertEnv(var, testCondition);
            }
        }
    }

    @Test
    public void ensureUnder1GBIsLessThan256MB() {
        int ram = 512;
        int heap = -1;
        Mockito.when(configuration.getMem()).thenReturn((double) ram);
        Mockito.when(configuration.getHeap()).thenReturn((double) heap);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for (Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.ES_JAVA_OPTS)) {
                final Consumer<Matcher> testCondition = matcher -> {
                    assertTrue(matcher.matches());
                    assertEquals(Integer.toString(ram - ram / 4), matcher.group(1));
                    assertTrue(ram - Integer.parseInt(matcher.group(1)) < 256);
                    assertTrue(ram - Integer.parseInt(matcher.group(1)) < ram);
                    assertTrue(ram - Integer.parseInt(matcher.group(1)) > 0);
                };

                assertEnv(var, testCondition);
            }
        }
    }

    @Test
    public void ensureHeapReturnedIfSpecified() {
        int ram = 512;
        int heap = 384;
        Mockito.when(configuration.getMem()).thenReturn((double) ram);
        Mockito.when(configuration.getHeap()).thenReturn((double) heap);
        ExecutorEnvironmentalVariables env = new ExecutorEnvironmentalVariables(configuration);

        for (Protos.Environment.Variable var : env.getList()) {
            if (var.getName().equals(ExecutorEnvironmentalVariables.ES_JAVA_OPTS)) {
                final Consumer<Matcher> testCondition = matcher -> {
                    assertTrue(matcher.matches());
                    assertEquals(Integer.toString(heap), matcher.group(1));
                };

                assertEnv(var, testCondition);
            }
        }
    }

    private static void assertEnv(
      final Protos.Environment.Variable var,
      final Consumer<Matcher> testCondition
    ) {
        final String[] tokens = var.getValue().split(" ");
        assertEquals(tokens.length, 3);
        final String minVal = tokens[0].substring("-Xms".length());
        final String maxVal = tokens[1].substring("-Xmx".length());
        final Pattern pattern = Pattern.compile("(\\d*)m");

        testCondition.accept(pattern.matcher(minVal));
        testCondition.accept(pattern.matcher(maxVal));

        assertEquals(tokens[2], "-Des.allow_insecure_settings=true");
    }
}
