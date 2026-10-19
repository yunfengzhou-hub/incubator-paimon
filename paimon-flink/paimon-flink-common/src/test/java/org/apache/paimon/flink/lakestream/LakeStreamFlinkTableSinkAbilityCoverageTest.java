/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.flink.sink.FlinkTableSink;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the contract {@link LakeStreamFlinkTableSink} inherits into existence.
 *
 * <p>The planner discovers sink abilities with {@code instanceof}, and a subclass inherits every
 * ability its parent declares whether or not it can honour it. An inherited ability that is left to
 * the parent's implementation would configure the Paimon side while the Lakestream Store executes
 * the statement, so every ability method has to be overridden and answered by the side that runs.
 *
 * <p>This test is what keeps that true when {@link FlinkTableSink} grows an ability: without it the
 * new ability would be advertised by inheritance and silently handled by the wrong side. It covers
 * every interface in the hierarchy, not just the {@code sink.abilities} package, because the
 * planner also probes interfaces such as {@code StatefulSink}.
 */
public class LakeStreamFlinkTableSinkAbilityCoverageTest {

    @Test
    public void testInheritedSinkIsNotAPlainPaimonSink() {
        // the routing sink really is a Paimon sink, which is where the inherited abilities come
        // from
        assertThat(FlinkTableSink.class).isAssignableFrom(LakeStreamFlinkTableSink.class);
    }

    /**
     * Ability methods deliberately left to the inherited implementation, with the reason. Anything
     * not listed here has to be overridden, so a new ability cannot be inherited unnoticed.
     */
    private static final Set<String> INHERITED_ON_PURPOSE =
            new HashSet<>(
                    Collections.singletonList(
                            // sets exactly the flag LakeStreamFlinkTableSink branches on; not
                            // forwarded to the store, which never executes an overwrite
                            "SupportsOverwrite#applyOverwrite"));

    @Test
    public void testEverySinkAbilityMethodIsOverridden() {
        Set<Class<?>> inspected = probeableInterfacesOf(FlinkTableSink.class);
        assertThat(inspected)
                .as("the interfaces Flink can probe for through instanceof")
                .contains(DynamicTableSink.class)
                .contains(SupportsOverwrite.class);

        List<String> notOverridden = new ArrayList<>();
        for (Class<?> iface : inspected) {
            for (Method method : iface.getDeclaredMethods()) {
                if (method.isSynthetic() || Modifier.isStatic(method.getModifiers())) {
                    continue;
                }
                String key = iface.getSimpleName() + "#" + method.getName();
                if (INHERITED_ON_PURPOSE.contains(key)) {
                    continue;
                }
                Method resolved = resolve(method);
                if (resolved == null) {
                    notOverridden.add(key + " (missing)");
                } else if (resolved.getDeclaringClass() != LakeStreamFlinkTableSink.class) {
                    notOverridden.add(
                            key + " -> handled by " + resolved.getDeclaringClass().getSimpleName());
                }
            }
        }

        assertThat(notOverridden)
                .as(
                        "LakeStreamFlinkTableSink inherits these abilities from FlinkTableSink but "
                                + "leaves them to Paimon's implementation, so the planner would "
                                + "configure Paimon while the Lakestream Store executes the "
                                + "statement. Override each of them and answer it from whichever "
                                + "side runs, reject it explicitly, or add it to "
                                + "INHERITED_ON_PURPOSE with the reason.")
                .isEmpty();
    }

    private static Method resolve(Method method) {
        try {
            return LakeStreamFlinkTableSink.class.getMethod(
                    method.getName(), method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Every interface in {@code FlinkTableSink}'s hierarchy that Flink could probe for. Not just
     * the {@code sink.abilities} package: the planner also checks interfaces such as {@code
     * StatefulSink} and {@code TwoPhaseCommittingSink}, which live directly in {@code
     * connector.sink}.
     */
    private static Set<Class<?>> probeableInterfacesOf(Class<?> sinkClass) {
        Set<Class<?>> found = new LinkedHashSet<>();
        collect(sinkClass, found);
        return found;
    }

    private static void collect(Class<?> type, Set<Class<?>> found) {
        for (Class<?> iface : type.getInterfaces()) {
            if (!iface.getName().startsWith("java.")) {
                found.add(iface);
            }
            collect(iface, found);
        }
        Class<?> superClass = type.getSuperclass();
        if (superClass != null) {
            collect(superClass, found);
        }
    }
}
