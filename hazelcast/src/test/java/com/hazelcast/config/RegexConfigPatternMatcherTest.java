/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.config.matcher.RegexConfigPatternMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RegexConfigPatternMatcherTest {

    @Test
    public void testQueueConfigWithoutWildcard() {
        QueueConfig queueConfig = new QueueConfig().setName("^someQueue$");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addQueueConfig(queueConfig);

        assertEquals(queueConfig, config.getQueueConfig("someQueue"));
        assertEquals(queueConfig, config.getQueueConfig("someQueue@foo"));

        // non-matching name
        assertNotEquals(queueConfig, config.getQueueConfig("doesNotExist"));
        // non-matching name (starts with)
        assertNotEquals(queueConfig, config.getQueueConfig("_someQueue"));
        // non-matching name (ends with)
        assertNotEquals(queueConfig, config.getQueueConfig("someQueue_"));
        // non-matching case
        assertNotEquals(queueConfig, config.getQueueConfig("SomeQueue"));
    }

    @Test
    public void testQueueConfigRegexContains() {
        QueueConfig queueConfig = new QueueConfig().setName("abc");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addQueueConfig(queueConfig);

        assertEquals(queueConfig, config.getQueueConfig("abcD"));
        assertNotEquals(queueConfig, config.getQueueConfig("abDD"));
    }

    @Test
    public void testQueueConfigRegexStartsWith() {
        QueueConfig queueConfig = new QueueConfig().setName("^abc");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addQueueConfig(queueConfig);

        assertEquals(queueConfig, config.getQueueConfig("abcDe"));
        assertNotEquals(queueConfig, config.getQueueConfig("bcDe"));
    }

    @Test
    public void testMapConfigWithoutWildcard() {
        MapConfig mapConfig = new MapConfig().setName("^someMap$");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        assertEquals(mapConfig, config.getMapConfiguration("someMap"));
        assertEquals(mapConfig, config.getMapConfiguration("someMap@foo"));

        // non-matching name
        assertNotEquals(mapConfig, config.getMapConfiguration("doesNotExist"));
        // non-matching name (starts with)
        assertNotEquals(mapConfig, config.getMapConfiguration("_someMap"));
        // non-matching name (ends with)
        assertNotEquals(mapConfig, config.getMapConfiguration("someMap_"));
        // non-matching case
        assertNotEquals(mapConfig, config.getMapConfiguration("SomeMap"));
    }

    @Test
    public void testMapConfigCaseInsensitive() {
        MapConfig mapConfig = new MapConfig().setName("^someMap$");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher(Pattern.CASE_INSENSITIVE));
        config.addMapConfig(mapConfig);

        // case insensitive matching
        assertEquals(mapConfig, config.getMapConfiguration("SomeMap"));

        // non-matching name (starts with)
        assertNotEquals(mapConfig, config.getMapConfiguration("_SomeMap"));
        // non-matching name (ends with)
        assertNotEquals(mapConfig, config.getMapConfiguration("SomeMap_"));
    }

    @Test
    public void testMapConfigContains() {
        MapConfig mapConfig = new MapConfig().setName("bc");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfiguration("bc.xyz"));
        assertEquals(mapConfig, config.getMapConfiguration("bc.xyz@foo"));

        // we should also match this (contains)
        assertEquals(mapConfig, config.getMapConfiguration("abc.xyz"));
        assertEquals(mapConfig, config.getMapConfiguration("abc.xyz@foo"));
    }

    @Test
    public void testMapConfigStartsWith() {
        MapConfig mapConfig = new MapConfig().setName("^abc");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfiguration("abc"));
        assertEquals(mapConfig, config.getMapConfiguration("abc@foo"));
        assertEquals(mapConfig, config.getMapConfiguration("abc.xyz"));
        assertEquals(mapConfig, config.getMapConfiguration("abc.xyz@foo"));

        // we should not match this (starts-with)
        assertNotEquals(mapConfig, config.getMapConfiguration("bc"));
        assertNotEquals(mapConfig, config.getMapConfiguration("bc@foo"));
        assertNotEquals(mapConfig, config.getMapConfiguration("bc.xyz"));
        assertNotEquals(mapConfig, config.getMapConfiguration("bc.xyz@foo"));
    }

    @Test
    public void testMapConfigEndsWith() {
        MapConfig mapConfig = new MapConfig().setName("bc$");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfiguration("abc"));
        assertEquals(mapConfig, config.getMapConfiguration("abc@foo"));
        assertEquals(mapConfig, config.getMapConfiguration("xyz.abc"));
        assertEquals(mapConfig, config.getMapConfiguration("xyz.abc@foo"));

        // we should not match this (ends-with)
        assertNotEquals(mapConfig, config.getMapConfiguration("abcD"));
        assertNotEquals(mapConfig, config.getMapConfiguration("abcD@foo"));
        assertNotEquals(mapConfig, config.getMapConfiguration("xyz.abcD"));
        assertNotEquals(mapConfig, config.getMapConfiguration("xyz.abcD@foo"));
    }
}
