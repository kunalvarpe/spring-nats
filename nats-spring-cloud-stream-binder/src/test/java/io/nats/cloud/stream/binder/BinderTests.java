/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nats.cloud.stream.binder;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.Headers;
import io.nats.cloud.stream.binder.properties.NatsBindingProperties;
import io.nats.cloud.stream.binder.properties.NatsBinderConfigurationProperties;
import io.nats.cloud.stream.binder.properties.NatsConsumerProperties;
import io.nats.cloud.stream.binder.properties.NatsExtendedBindingProperties;
import io.nats.cloud.stream.binder.properties.NatsProducerProperties;
import io.nats.spring.boot.autoconfigure.NatsAutoConfiguration;
import io.nats.spring.boot.autoconfigure.NatsProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.EmbeddedHeaderUtils;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class BinderTests {
    private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(5);
    private static final String SHARED_TLS_RESOURCES = "../nats-spring/src/test/resources/";
    private static final String TRACE_HEADER = "x-trace-id";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(NatsAutoConfiguration.class));
    private final ApplicationContextRunner binderContextRunner = new ApplicationContextRunner()
            .withUserConfiguration(NatsChannelBinderConfiguration.class);

    @Test
    void createBinderFromGlobalProperties() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.run(context -> {
                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    assertConnected(fixture.connection(), ts.getURI());
                    assertDefaultListenersCanHandleCallbacks(fixture.connection());
                    assertThat(fixture.binder().getDefaultsPrefix()).isEqualTo("nats.spring.cloud.stream.default");
                    assertThat(fixture.binder().getExtendedPropertiesEntryClass()).isEqualTo(NatsBindingProperties.class);
                    assertThat(fixture.binder().getExtendedConsumerProperties("input")).isNotNull();
                    assertThat(fixture.binder().getExtendedProducerProperties("output")).isNotNull();
                }
            });
        }
    }

    @Test
    void createBinderFromBinderProperties() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.run(context -> {
                try (BinderFixture fixture = newBinderPropertiesBinder(ts.getURI())) {
                    assertConnected(fixture.connection(), ts.getURI());
                }
            });
        }
    }

    @Test
    void createBinderWithoutServerProperties() throws IOException, InterruptedException {
        NatsChannelBinderConfiguration config = new NatsChannelBinderConfiguration();
        NatsChannelProvisioner provisioner = config.natsChannelProvisioner();
        config.setNatsProperties(new NatsProperties());
        config.setNatsBinderConfigurationProperties(new NatsBinderConfigurationProperties());
        config.setNatsExtendedBindingProperties(new NatsExtendedBindingProperties());

        assertThat(config.natsBinder(provisioner)).isNull();
    }

    @Test
    void createBinderWithoutConfigurationObjectsLeavesConnectionUnset() {
        NatsChannelBinder binder = new NatsChannelBinder(
                new NatsExtendedBindingProperties(),
                null,
                null,
                new NatsChannelProvisioner(),
                null,
                null);

        assertThat(binder.getConnection()).isNull();
    }

    @Test
    void createBinderWithBlankServerPropertiesLeavesConnectionUnset() {
        NatsBinderConfigurationProperties binderProps = new NatsBinderConfigurationProperties();
        binderProps.setServer("");
        NatsProperties natsProperties = new NatsProperties();
        natsProperties.setServer("");
        NatsChannelBinder binder = new NatsChannelBinder(
                new NatsExtendedBindingProperties(),
                binderProps,
                natsProperties,
                new NatsChannelProvisioner(),
                null,
                null);

        assertThat(binder.getConnection()).isNull();
    }

    @Test
    void binderConfigurationExposesAccessorsAndDefaultMappings() {
        NatsChannelBinderConfiguration config = new NatsChannelBinderConfiguration();
        NatsBinderConfigurationProperties binderProps = new NatsBinderConfigurationProperties();
        NatsExtendedBindingProperties extendedProps = new NatsExtendedBindingProperties();
        NatsProperties natsProperties = new NatsProperties();

        config.setNatsBinderConfigurationProperties(binderProps);
        config.setNatsExtendedBindingProperties(extendedProps);
        config.setNatsProperties(natsProperties);

        ConfigurationPropertyName streamPrefix = ConfigurationPropertyName.of("nats.spring.cloud.stream");
        Map<ConfigurationPropertyName, ConfigurationPropertyName> mappings =
                config.natsExtendedPropertiesDefaultMappingsProvider().getDefaultMappings();

        assertThat(config.getNatsBinderConfigurationProperties()).isSameAs(binderProps);
        assertThat(config.getNatsExtendedBindingProperties()).isSameAs(extendedProps);
        assertThat(config.getNatsProperties()).isSameAs(natsProperties);
        assertThat(config.natsChannelProvisioner()).isNotNull();
        assertThat(mappings).containsEntry(streamPrefix, streamPrefix);
    }

    @Test
    void binderConfigurationBindsCustomEmbeddedHeaders() {
        this.binderContextRunner
                .withPropertyValues("nats.spring.cloud.stream.binder.headers-to-embed=" + TRACE_HEADER + ",x-tenant-id")
                .run(context -> {
                    NatsBinderConfigurationProperties properties =
                            context.getBean(NatsBinderConfigurationProperties.class);

                    assertThat(properties.getHeadersToEmbed()).containsExactly(TRACE_HEADER, "x-tenant-id");
                });
    }

    @Test
    void testServerStartFailsWhenPortIsOwnedByNonNatsListener() throws IOException {
        try (ServerSocket listener = new ServerSocket(0)) {
            assertThatThrownBy(() -> new NatsBinderTestServer(listener.getLocalPort(), false))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("nats-server");
        }
    }

    @Test
    void configBackedServerStartFailureDeletesGeneratedConfig() throws IOException {
        String previousServerPath = System.getProperty("nats_server_path");
        Set<Path> before = tempNatsConfigs();
        System.setProperty("nats_server_path", missingExecutablePath());

        try {
            assertThatThrownBy(() -> new NatsBinderTestServer(SHARED_TLS_RESOURCES + "tls.conf", false))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to run");
            assertThat(tempNatsConfigs()).isEqualTo(before);
        } finally {
            restoreServerPath(previousServerPath);
        }
    }

    @Test
    void createBinderReturnsNullWhenAuthenticationFails() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer(new String[]{"--auth", "secret"}, false)) {
            NatsChannelBinderConfiguration config = new NatsChannelBinderConfiguration();
            NatsChannelProvisioner provisioner = config.natsChannelProvisioner();
            NatsBinderConfigurationProperties binderProps = new NatsBinderConfigurationProperties();
            binderProps.setServer(ts.getURI());
            binderProps.setConnectionTimeout(Duration.ofSeconds(1));
            config.setNatsProperties(new NatsProperties());
            config.setNatsBinderConfigurationProperties(binderProps);
            config.setNatsExtendedBindingProperties(new NatsExtendedBindingProperties());

            assertThat(config.natsBinder(provisioner)).isNull();
        }
    }

    @Test
    void createBinderReturnsNullWhenServerIsUnreachable() throws IOException, InterruptedException {
        int unusedPort = NatsBinderTestServer.nextPort();
        NatsChannelBinderConfiguration config = new NatsChannelBinderConfiguration();
        NatsChannelProvisioner provisioner = config.natsChannelProvisioner();
        NatsBinderConfigurationProperties binderProps = new NatsBinderConfigurationProperties();
        binderProps.setServer("nats://127.0.0.1:" + unusedPort);
        binderProps.setConnectionTimeout(Duration.ofMillis(250));
        config.setNatsProperties(new NatsProperties());
        config.setNatsBinderConfigurationProperties(binderProps);
        config.setNatsExtendedBindingProperties(new NatsExtendedBindingProperties());

        assertThat(config.natsBinder(provisioner)).isNull();
    }

    @Test
    void createTLSBinderFromGlobalProperties() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer(SHARED_TLS_RESOURCES + "tls.conf", false)) {
            this.contextRunner.run(context -> {
                try (BinderFixture fixture = newTlsGlobalBinder(ts.getURI())) {
                    assertConnected(fixture.connection(), ts.getURI());
                }
            });
        }
    }

    @Test
    void createBinderWithCustomListeners() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            AtomicReference<ConnectionListener.Events> event = new AtomicReference<>();
            NatsChannelBinder binder = new NatsChannelBinder(
                    new NatsExtendedBindingProperties(),
                    new NatsBinderConfigurationProperties(),
                    (NatsProperties) new NatsProperties().server(ts.getURI()),
                    new NatsChannelProvisioner(),
                    (connection, type) -> event.set(type),
                    new ErrorListener() {
                        @Override
                        public void slowConsumerDetected(Connection conn, Consumer consumer) {
                        }

                        @Override
                        public void exceptionOccurred(Connection conn, Exception exp) {
                        }

                        @Override
                        public void errorOccurred(Connection conn, String error) {
                        }
                    });

            try {
                assertConnected(binder.getConnection(), ts.getURI());
                await().atMost(5, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(event.get()).isNotNull());
            } finally {
                binder.getConnection().close();
            }
        }
    }

    @Test
    void springContextCreatesBinderFromBinderProperties() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.binderContextRunner.withPropertyValues(
                    "nats.spring.cloud.stream.binder.server=" + ts.getURI()).run(context -> {
                assertThat(context).hasSingleBean(NatsChannelBinder.class);
                NatsChannelBinder binder = context.getBean(NatsChannelBinder.class);
                assertConnected(binder.getConnection(), ts.getURI());
                binder.getConnection().close();
            });
        }
    }

    @Test
    void springContextCreatesBinderFromGlobalProperties() throws IOException, InterruptedException {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.binderContextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                assertThat(context).hasSingleBean(NatsChannelBinder.class);
                assertConnected(context.getBean(Connection.class), ts.getURI());

                NatsChannelBinder binder = context.getBean(NatsChannelBinder.class);
                assertConnected(binder.getConnection(), ts.getURI());
                binder.getConnection().close();
            });
        }
    }

    @Test
    void binderPropertiesWinOverGlobalProperties() throws IOException, InterruptedException {
        try (NatsBinderTestServer global = new NatsBinderTestServer();
             NatsBinderTestServer binderSpecific = new NatsBinderTestServer()) {
            this.binderContextRunner.withPropertyValues(
                    "nats.spring.server=" + global.getURI(),
                    "nats.spring.cloud.stream.binder.server=" + binderSpecific.getURI()).run(context -> {
                Connection globalConnection = context.getBean(Connection.class);
                NatsChannelBinder binder = context.getBean(NatsChannelBinder.class);
                try {
                    assertConnected(globalConnection, global.getURI());
                    assertConnected(binder.getConnection(), binderSpecific.getURI());
                } finally {
                    globalConnection.close();
                    binder.getConnection().close();
                }
            });
        }
    }

    @Test
    void testMessageProducer() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String theMessage = "hello world";
                    String in = "in";

                    ConsumerDestination from = fixture.provisioner().provisionConsumerDestination(in, "", null);
                    NatsMessageProducer producer = (NatsMessageProducer) fixture.binder().createConsumerEndpoint(from, "", null);
                    CompletableFuture<String> received = new CompletableFuture<>();
                    DirectChannel output = new DirectChannel();
                    output.subscribe(msg -> received.complete(payloadText(msg.getPayload())));
                    producer.setOutputChannel(output);

                    assertThat(producer.getOutputChannel()).isSameAs(output);
                    assertThat(producer.isRunning()).isFalse();

                    try {
                        producer.start();
                        assertThat(producer.isRunning()).isTrue();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        conn.publish(in, theMessage.getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        assertThat(received.get(5, TimeUnit.SECONDS)).isEqualTo(theMessage);
                    } finally {
                        producer.stop();
                    }
                }
            });
        }
    }

    @Test
    void messageProducerStartAndStopAreIdempotent() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "producer.lifecycle";
                    ConsumerDestination from = fixture.provisioner().provisionConsumerDestination(subject, "", null);
                    NatsMessageProducer producer =
                            (NatsMessageProducer) fixture.binder().createConsumerEndpoint(from, "", null);
                    CompletableFuture<String> received = new CompletableFuture<>();
                    DirectChannel output = new DirectChannel();
                    output.subscribe(msg -> received.complete(payloadText(msg.getPayload())));
                    producer.setOutputChannel(output);

                    producer.start();
                    producer.start();
                    assertThat(producer.isRunning()).isTrue();
                    fixture.connection().flush(FLUSH_TIMEOUT);

                    conn.publish(subject, "once".getBytes(UTF_8));
                    conn.flush(FLUSH_TIMEOUT);

                    assertThat(received.get(5, TimeUnit.SECONDS)).isEqualTo("once");

                    producer.stop();
                    producer.stop();
                    assertThat(producer.isRunning()).isFalse();
                }
            });
        }
    }

    @Test
    void messageProducerWithoutOutputChannelKeepsRunningAndDropsMessage() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "producer.no.output";
                    ConsumerDestination from = fixture.provisioner().provisionConsumerDestination(subject, "", null);
                    NatsMessageProducer producer =
                            (NatsMessageProducer) fixture.binder().createConsumerEndpoint(from, "", null);

                    try {
                        producer.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        conn.publish(subject, "dropped".getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        await().atMost(1, TimeUnit.SECONDS)
                                .untilAsserted(() -> assertThat(producer.isRunning()).isTrue());
                    } finally {
                        producer.stop();
                    }
                }
            });
        }
    }

    @Test
    void messageProducerKeepsRunningWhenOutputChannelThrows() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "producer.output.failure";
                    ConsumerDestination from = fixture.provisioner().provisionConsumerDestination(subject, "", null);
                    NatsMessageProducer producer =
                            (NatsMessageProducer) fixture.binder().createConsumerEndpoint(from, "", null);
                    DirectChannel output = new DirectChannel();
                    output.subscribe(msg -> {
                        throw new IllegalStateException("channel failed");
                    });
                    producer.setOutputChannel(output);

                    try {
                        producer.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        conn.publish(subject, "fail safely".getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        await().atMost(5, TimeUnit.SECONDS)
                                .untilAsserted(() -> assertThat(producer.isRunning()).isTrue());
                    } finally {
                        producer.stop();
                    }
                }
            });
        }
    }

    @Test
    void testMessageProducerWithGroup() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String theMessage = "hello world";
                    String in = "in";
                    String group = "group";
                    int total = 100;

                    ConsumerDestination from = fixture.provisioner().provisionConsumerDestination(in, group, null);
                    AtomicInteger counter = new AtomicInteger(0);

                    NatsMessageProducer producer = buildCountingProducer(from, group, fixture, counter);
                    NatsMessageProducer producer2 = buildCountingProducer(from, group, fixture, counter);

                    try {
                        producer.start();
                        producer2.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        for (int i = 0; i < total; i++) {
                            conn.publish(in, theMessage.getBytes(UTF_8));
                        }
                        conn.flush(FLUSH_TIMEOUT);

                        await().atMost(5, TimeUnit.SECONDS)
                                .untilAsserted(() -> assertThat(counter.get()).isEqualTo(total));
                    } finally {
                        producer.stop();
                        producer2.stop();
                    }
                }
            });
        }
    }

    @Test
    void messageSourceStartAndStopAreIdempotent() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "source.lifecycle";
                    NatsConsumerDestination from =
                            (NatsConsumerDestination) fixture.provisioner().provisionConsumerDestination(subject, "", null);
                    NatsMessageSource src = new NatsMessageSource(from, fixture.connection());

                    assertThat(src.receive()).isNull();
                    src.stop();

                    src.start();
                    src.start();
                    assertThat(src.isRunning()).isTrue();
                    fixture.connection().flush(FLUSH_TIMEOUT);

                    CompletableFuture<org.springframework.messaging.Message<Object>> received =
                            CompletableFuture.supplyAsync(src::receive);

                    conn.publish(subject, "source-once".getBytes(UTF_8));
                    conn.flush(FLUSH_TIMEOUT);

                    assertThat(payloadText(received.get(5, TimeUnit.SECONDS).getPayload())).isEqualTo("source-once");

                    src.stop();
                    src.stop();
                    assertThat(src.isRunning()).isFalse();
                }
            });
        }
    }

    @Test
    void messageSourceReceiveReturnsNullWhenInterrupted() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    NatsConsumerDestination from = (NatsConsumerDestination) fixture.provisioner()
                            .provisionConsumerDestination("source.interrupted", "", null);
                    NatsMessageSource src = new NatsMessageSource(from, fixture.connection());
                    AtomicReference<org.springframework.messaging.Message<Object>> received = new AtomicReference<>();
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    CountDownLatch started = new CountDownLatch(1);
                    AtomicInteger done = new AtomicInteger(0);

                    try {
                        src.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);
                        assertThat(src.getComponentType()).isEqualTo("nats:message-source");

                        Thread receiver = new Thread(() -> {
                            started.countDown();
                            try {
                                received.set(src.receive());
                            } catch (Throwable exp) {
                                failure.set(exp);
                            } finally {
                                done.incrementAndGet();
                            }
                        });
                        receiver.start();
                        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                        receiver.interrupt();

                        await().atMost(5, TimeUnit.SECONDS)
                                .untilAsserted(() -> assertThat(done.get()).isEqualTo(1));
                        assertThat(failure.get()).isNull();
                        assertThat(received.get()).isNull();
                    } finally {
                        src.stop();
                    }
                }
            });
        }
    }

    @Test
    void testMessageSource() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String theMessage = "hello world";
                    String in = "in";

                    NatsConsumerDestination from =
                            (NatsConsumerDestination) fixture.provisioner().provisionConsumerDestination(in, "", null);
                    NatsMessageSource src = new NatsMessageSource(from, fixture.connection());

                    assertThat(src.isRunning()).isFalse();
                    try {
                        src.start();
                        assertThat(src.isRunning()).isTrue();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        CompletableFuture<org.springframework.messaging.Message<Object>> received =
                                CompletableFuture.supplyAsync(src::receive);

                        conn.publish(in, theMessage.getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        assertThat(payloadText(received.get(5, TimeUnit.SECONDS).getPayload())).isEqualTo(theMessage);
                    } finally {
                        src.stop();
                    }
                }
            });
        }
    }

    @Test
    void messageSourcePreservesNatsHeadersAsSpringHeadersForIssue65() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "source.headers.issue65";
                    String payload = "headers survive polling";
                    NatsConsumerDestination from =
                            (NatsConsumerDestination) fixture.provisioner().provisionConsumerDestination(subject, "", null);
                    NatsMessageSource src = new NatsMessageSource(from, fixture.connection());

                    try {
                        src.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        conn.publish(subject, headersWithTrace("poll-123"), payload.getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<Object> received = src.receive();
                        assertThat(received).isNotNull();
                        assertThat(payloadText(received.getPayload())).isEqualTo(payload);
                        assertThat(received.getHeaders()).containsEntry(TRACE_HEADER, "poll-123");
                    } finally {
                        src.stop();
                    }
                }
            });
        }
    }

    @Test
    void testMessageSourceWithQueue() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String theMessage = "hello world";
                    String in = "in";
                    String group = "group";

                    NatsConsumerDestination from =
                            (NatsConsumerDestination) fixture.provisioner().provisionConsumerDestination(in, group, null);
                    NatsMessageSource src = new NatsMessageSource(from, fixture.connection());

                    assertThat(src.isRunning()).isFalse();
                    try {
                        src.start();
                        assertThat(src.isRunning()).isTrue();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        CompletableFuture<org.springframework.messaging.Message<Object>> received =
                                CompletableFuture.supplyAsync(src::receive);

                        conn.publish(in, theMessage.getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        assertThat(payloadText(received.get(5, TimeUnit.SECONDS).getPayload())).isEqualTo(theMessage);
                    } finally {
                        src.stop();
                    }
                }
            });
        }
    }

    @Test
    void messageHandlerWithoutConnectionDropsSupportedPayload() {
        MessageHandler handler = new NatsMessageHandler("handler.no.connection", null);

        assertThatCode(() -> handler.handleMessage(new GenericMessage<>("ignored"))).doesNotThrowAnyException();
    }

    @Test
    void testMessageHandler() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String theMessage = "hello world";
                    String out = "out";
                    ProducerDestination to = fixture.provisioner().provisionProducerDestination(out, null);
                    MessageHandler mh = fixture.binder().createProducerMessageHandler(to, null, null);

                    Subscription sub = conn.subscribe(out);
                    conn.flush(FLUSH_TIMEOUT);

                    mh.handleMessage(new GenericMessage<>(theMessage.getBytes(UTF_8)));
                    fixture.connection().flush(FLUSH_TIMEOUT);
                    assertThat(nextText(sub)).isEqualTo(theMessage);

                    ByteBuffer buffer = ByteBuffer.wrap(theMessage.getBytes(UTF_8));
                    mh.handleMessage(new GenericMessage<>(buffer));
                    fixture.connection().flush(FLUSH_TIMEOUT);
                    assertThat(nextText(sub)).isEqualTo(theMessage);

                    mh.handleMessage(new GenericMessage<>(theMessage));
                    fixture.connection().flush(FLUSH_TIMEOUT);
                    assertThat(nextText(sub)).isEqualTo(theMessage);

                    mh.handleMessage(new GenericMessage<>(Integer.valueOf(2)));
                    fixture.connection().flush(FLUSH_TIMEOUT);
                    assertThat(sub.nextMessage(Duration.ofMillis(250))).isNull();
                }
            });
        }
    }

    @Test
    void messageHandlerPublishesCustomHeadersForIssue65() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "handler.headers.issue65";
                    String payload = "headers survive publish";
                    ProducerDestination to = fixture.provisioner().provisionProducerDestination(subject, null);
                    MessageHandler handler = fixture.binder().createProducerMessageHandler(to, null, null);

                    Subscription sub = conn.subscribe(subject);
                    conn.flush(FLUSH_TIMEOUT);

                    handler.handleMessage(MessageBuilder.withPayload(payload)
                            .setHeader(TRACE_HEADER, "send-123")
                            .setHeader("custom-number", 42)
                            .setHeader("custom-list", Arrays.asList("first", null, "second"))
                            .build());
                    fixture.connection().flush(FLUSH_TIMEOUT);

                    Message received = sub.nextMessage(FLUSH_TIMEOUT);
                    assertThat(received).isNotNull();
                    assertThat(new String(received.getData(), UTF_8)).isEqualTo(payload);
                    assertThat(received.hasHeaders()).isTrue();
                    assertThat(received.getHeaders().getFirst(TRACE_HEADER)).isEqualTo("send-123");
                    assertThat(received.getHeaders().getFirst("custom-number")).isEqualTo("42");
                    assertThat(received.getHeaders().get("custom-list")).containsExactly("first", "second");
                }
            });
        }
    }

    @Test
    void messageHandlerSkipsHeadersThatCannotBeMappedToNatsHeaders() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "handler.headers.skipped";
                    String payload = "only payload";
                    ProducerDestination to = fixture.provisioner().provisionProducerDestination(subject, null);
                    MessageHandler handler = fixture.binder().createProducerMessageHandler(to, null, null);

                    Subscription sub = conn.subscribe(subject);
                    conn.flush(FLUSH_TIMEOUT);

                    Map<String, Object> headers = new HashMap<>();
                    headers.put("", "blank");
                    headers.put(NatsMessageProducer.SUBJECT, "reserved");
                    headers.put(BinderHeaders.NATIVE_HEADERS_PRESENT, true);
                    headers.put("empty-custom", List.of());
                    headers.put("invalid-value", "bad\r\nvalue");

                    handler.handleMessage(new GenericMessage<>(payload, headers));
                    fixture.connection().flush(FLUSH_TIMEOUT);

                    Message received = sub.nextMessage(FLUSH_TIMEOUT);
                    assertThat(received).isNotNull();
                    assertThat(new String(received.getData(), UTF_8)).isEqualTo(payload);
                    assertThat(received.hasHeaders()).isFalse();
                }
            });
        }
    }

    @Test
    void messageProducerPreservesNatsHeadersAsSpringHeadersForIssue65() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String subject = "producer.headers.issue65";
                    String payload = "headers survive dispatch";
                    ConsumerDestination from = fixture.provisioner().provisionConsumerDestination(subject, "", null);
                    NatsMessageProducer producer =
                            (NatsMessageProducer) fixture.binder().createConsumerEndpoint(from, "", null);
                    CompletableFuture<org.springframework.messaging.Message<?>> received = new CompletableFuture<>();
                    DirectChannel output = new DirectChannel();
                    output.subscribe(received::complete);
                    producer.setOutputChannel(output);

                    try {
                        producer.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        Headers headers = headersWithTrace("dispatch-123")
                                .put("custom-list", List.of("first", "second"))
                                .put("Subject", "spoofed.subject")
                                .put("replyChannel", "spoofed.reply");
                        conn.publish(subject, "actual.reply", headers, payload.getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<?> message = received.get(5, TimeUnit.SECONDS);
                        assertThat(payloadText(message.getPayload())).isEqualTo(payload);
                        assertThat(message.getHeaders()).containsEntry(TRACE_HEADER, "dispatch-123");
                        assertThat(message.getHeaders()).containsEntry("custom-list", List.of("first", "second"));
                        assertThat(message.getHeaders()).containsEntry(NatsMessageProducer.SUBJECT, subject);
                        assertThat(message.getHeaders()).containsEntry(MessageHeaders.REPLY_CHANNEL, "actual.reply");
                    } finally {
                        producer.stop();
                    }
                }
            });
        }
    }

    @Test
    void customHeadersRoundTripThroughBindProducerAndBindConsumerForIssue65() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.headers.issue65";
                    String payload = "headers survive round trip";
                    DirectChannel output = new DirectChannel();
                    DirectChannel input = new DirectChannel();
                    CompletableFuture<org.springframework.messaging.Message<?>> received = new CompletableFuture<>();
                    input.subscribe(received::complete);
                    Binding<MessageChannel> consumerBinding = null;
                    Binding<MessageChannel> producerBinding = null;

                    try {
                        consumerBinding = fixture.binder().bindConsumer(subject, "", input,
                                new ExtendedConsumerProperties<>(new NatsConsumerProperties()));
                        producerBinding = fixture.binder().bindProducer(subject, output,
                                new ExtendedProducerProperties<>(new NatsProducerProperties()));
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        output.send(MessageBuilder.withPayload(payload)
                                .setHeader(TRACE_HEADER, "roundtrip-123")
                                .build());
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<?> message = received.get(5, TimeUnit.SECONDS);
                        assertThat(payloadText(message.getPayload())).isEqualTo(payload);
                        assertThat(message.getHeaders()).containsEntry(TRACE_HEADER, "roundtrip-123");
                    } finally {
                        if (producerBinding != null) {
                            producerBinding.unbind();
                        }
                        if (consumerBinding != null) {
                            consumerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void embeddedHeaderModeEmbedsStandardHeadersInPayloadForIssue13() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.embedded.headers.issue13.raw";
                    String payload = "embedded header payload";
                    DirectChannel output = new DirectChannel();
                    ExtendedProducerProperties<NatsProducerProperties> producerProperties =
                            new ExtendedProducerProperties<>(new NatsProducerProperties());
                    producerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
                    Binding<MessageChannel> producerBinding = null;
                    Subscription sub = conn.subscribe(subject);

                    try {
                        producerBinding = fixture.binder().bindProducer(subject, output, producerProperties);
                        conn.flush(FLUSH_TIMEOUT);

                        output.send(MessageBuilder.withPayload(payload.getBytes(UTF_8))
                                .setHeader(MessageHeaders.CONTENT_TYPE, "text/plain")
                                .build());
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        Message raw = sub.nextMessage(FLUSH_TIMEOUT);
                        assertThat(raw).isNotNull();
                        assertThat(raw.hasHeaders()).isFalse();
                        assertThat(EmbeddedHeaderUtils.mayHaveEmbeddedHeaders(raw.getData())).isTrue();

                        MessageValues extracted = EmbeddedHeaderUtils.extractHeaders(raw.getData());
                        assertThat(payloadText(extracted.getPayload())).isEqualTo(payload);
                        assertThat(extracted.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("text/plain");
                    } finally {
                        if (producerBinding != null) {
                            producerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void embeddedHeaderModeEmbedsConfiguredCustomHeadersForIssue13() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI(), TRACE_HEADER)) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.embedded.headers.issue13.custom";
                    String payload = "embedded custom header payload";
                    DirectChannel output = new DirectChannel();
                    ExtendedProducerProperties<NatsProducerProperties> producerProperties =
                            new ExtendedProducerProperties<>(new NatsProducerProperties());
                    producerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
                    Binding<MessageChannel> producerBinding = null;
                    Subscription sub = conn.subscribe(subject);

                    try {
                        producerBinding = fixture.binder().bindProducer(subject, output, producerProperties);
                        conn.flush(FLUSH_TIMEOUT);

                        output.send(MessageBuilder.withPayload(payload.getBytes(UTF_8))
                                .setHeader(MessageHeaders.CONTENT_TYPE, "text/plain")
                                .setHeader(TRACE_HEADER, "trace-embedded")
                                .build());
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        Message raw = sub.nextMessage(FLUSH_TIMEOUT);
                        assertThat(raw).isNotNull();
                        assertThat(raw.hasHeaders()).isFalse();

                        MessageValues extracted = EmbeddedHeaderUtils.extractHeaders(raw.getData());
                        assertThat(payloadText(extracted.getPayload())).isEqualTo(payload);
                        assertThat(extracted.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("text/plain");
                        assertThat(extracted.get(TRACE_HEADER)).isEqualTo("trace-embedded");
                    } finally {
                        if (producerBinding != null) {
                            producerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void embeddedHeaderModeRoundTripsThroughProducerAndConsumerBindingsForIssue13() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.embedded.headers.issue13.roundtrip";
                    String payload = "embedded header round trip";
                    DirectChannel output = new DirectChannel();
                    DirectChannel input = new DirectChannel();
                    CompletableFuture<org.springframework.messaging.Message<?>> received = new CompletableFuture<>();
                    input.subscribe(received::complete);
                    ExtendedProducerProperties<NatsProducerProperties> producerProperties =
                            new ExtendedProducerProperties<>(new NatsProducerProperties());
                    producerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
                    ExtendedConsumerProperties<NatsConsumerProperties> consumerProperties =
                            new ExtendedConsumerProperties<>(new NatsConsumerProperties());
                    consumerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
                    Binding<MessageChannel> producerBinding = null;
                    Binding<MessageChannel> consumerBinding = null;

                    try {
                        consumerBinding = fixture.binder().bindConsumer(subject, "", input, consumerProperties);
                        producerBinding = fixture.binder().bindProducer(subject, output, producerProperties);
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        output.send(MessageBuilder.withPayload(payload.getBytes(UTF_8))
                                .setHeader(MessageHeaders.CONTENT_TYPE, "text/plain")
                                .build());
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<?> message = received.get(5, TimeUnit.SECONDS);
                        assertThat(payloadText(message.getPayload())).isEqualTo(payload);
                        assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("text/plain");
                    } finally {
                        if (producerBinding != null) {
                            producerBinding.unbind();
                        }
                        if (consumerBinding != null) {
                            consumerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void embeddedHeaderModeRestoresStandardHeadersFromPayloadForIssue13() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.embedded.headers.issue13.consumer";
                    String payload = "embedded header consumer";
                    DirectChannel input = new DirectChannel();
                    CompletableFuture<org.springframework.messaging.Message<?>> received = new CompletableFuture<>();
                    input.subscribe(received::complete);
                    ExtendedConsumerProperties<NatsConsumerProperties> consumerProperties =
                            new ExtendedConsumerProperties<>(new NatsConsumerProperties());
                    consumerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
                    Binding<MessageChannel> consumerBinding = null;

                    try {
                        consumerBinding = fixture.binder().bindConsumer(subject, "", input, consumerProperties);
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        MessageValues values = new MessageValues(payload.getBytes(UTF_8), Map.of(
                                MessageHeaders.CONTENT_TYPE, "text/plain"
                        ));
                        byte[] embeddedPayload = EmbeddedHeaderUtils.embedHeaders(
                                values,
                                EmbeddedHeaderUtils.headersToEmbed(null)
                        );
                        conn.publish(subject, embeddedPayload);
                        conn.flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<?> message = received.get(5, TimeUnit.SECONDS);
                        assertThat(payloadText(message.getPayload())).isEqualTo(payload);
                        assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("text/plain");
                    } finally {
                        if (consumerBinding != null) {
                            consumerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void headerModeNoneDoesNotPublishNativeNatsHeaders() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.headers.none.producer";
                    String payload = "headers suppressed";
                    DirectChannel output = new DirectChannel();
                    ExtendedProducerProperties<NatsProducerProperties> producerProperties =
                            new ExtendedProducerProperties<>(new NatsProducerProperties());
                    producerProperties.setHeaderMode(HeaderMode.none);
                    Binding<MessageChannel> producerBinding = null;
                    Subscription sub = conn.subscribe(subject);

                    try {
                        producerBinding = fixture.binder().bindProducer(subject, output, producerProperties);
                        conn.flush(FLUSH_TIMEOUT);

                        output.send(MessageBuilder.withPayload(payload.getBytes(UTF_8))
                                .setHeader(TRACE_HEADER, "suppressed")
                                .build());
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        Message raw = sub.nextMessage(FLUSH_TIMEOUT);
                        assertThat(raw).isNotNull();
                        assertThat(new String(raw.getData(), UTF_8)).isEqualTo(payload);
                        assertThat(raw.hasHeaders()).isFalse();
                    } finally {
                        if (producerBinding != null) {
                            producerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void headerModeNoneDoesNotExposeNativeNatsHeadersToConsumer() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.headers.none.consumer";
                    String payload = "headers hidden";
                    DirectChannel input = new DirectChannel();
                    CompletableFuture<org.springframework.messaging.Message<?>> received = new CompletableFuture<>();
                    input.subscribe(received::complete);
                    ExtendedConsumerProperties<NatsConsumerProperties> consumerProperties =
                            new ExtendedConsumerProperties<>(new NatsConsumerProperties());
                    consumerProperties.setHeaderMode(HeaderMode.none);
                    Binding<MessageChannel> consumerBinding = null;

                    try {
                        consumerBinding = fixture.binder().bindConsumer(subject, "", input, consumerProperties);
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        conn.publish(subject, headersWithTrace("hidden"), payload.getBytes(UTF_8));
                        conn.flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<?> message = received.get(5, TimeUnit.SECONDS);
                        assertThat(payloadText(message.getPayload())).isEqualTo(payload);
                        assertThat(message.getHeaders()).doesNotContainKey(TRACE_HEADER);
                        assertThat(message.getHeaders()).doesNotContainKey(BinderHeaders.NATIVE_HEADERS_PRESENT);
                    } finally {
                        if (consumerBinding != null) {
                            consumerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void embeddedHeaderModeDoesNotParsePayloadWhenNativeNatsHeadersArePresent() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    fixture.binder().setApplicationContext(context.getSourceApplicationContext(GenericApplicationContext.class));
                    String subject = "binder.embedded.headers.native.present";
                    String payload = "payload that should stay embedded";
                    DirectChannel input = new DirectChannel();
                    CompletableFuture<org.springframework.messaging.Message<?>> received = new CompletableFuture<>();
                    input.subscribe(received::complete);
                    ExtendedConsumerProperties<NatsConsumerProperties> consumerProperties =
                            new ExtendedConsumerProperties<>(new NatsConsumerProperties());
                    consumerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
                    Binding<MessageChannel> consumerBinding = null;

                    try {
                        consumerBinding = fixture.binder().bindConsumer(subject, "", input, consumerProperties);
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        MessageValues values = new MessageValues(payload.getBytes(UTF_8), Map.of(
                                MessageHeaders.CONTENT_TYPE, "text/plain"
                        ));
                        byte[] embeddedPayload = EmbeddedHeaderUtils.embedHeaders(
                                values,
                                EmbeddedHeaderUtils.headersToEmbed(null)
                        );
                        conn.publish(subject, headersWithTrace("native"), embeddedPayload);
                        conn.flush(FLUSH_TIMEOUT);

                        org.springframework.messaging.Message<?> message = received.get(5, TimeUnit.SECONDS);
                        assertThat(message.getPayload()).isEqualTo(embeddedPayload);
                        assertThat(message.getHeaders()).containsEntry(BinderHeaders.NATIVE_HEADERS_PRESENT, true);
                        assertThat(message.getHeaders()).doesNotContainKey(TRACE_HEADER);
                        assertThat(message.getHeaders()).doesNotContainKey(MessageHeaders.CONTENT_TYPE);
                    } finally {
                        if (consumerBinding != null) {
                            consumerBinding.unbind();
                        }
                    }
                }
            });
        }
    }

    @Test
    void testRequestReply() throws Exception {
        try (NatsBinderTestServer ts = new NatsBinderTestServer()) {
            this.contextRunner.withPropertyValues("nats.spring.server=" + ts.getURI()).run(context -> {
                Connection conn = context.getBean(Connection.class);
                assertConnected(conn, ts.getURI());

                try (BinderFixture fixture = newGlobalBinder(ts.getURI())) {
                    String request = "hello request";
                    String reply = "hello reply";
                    String req2rep = "req2rep";
                    String rep2req = "rep2req";
                    MessageHandler appOneMh =
                            fixture.binder().createProducerMessageHandler(
                                    fixture.provisioner().provisionProducerDestination(req2rep, null), null, null);
                    CompletableFuture<org.springframework.messaging.Message<?>> appOneReceived = new CompletableFuture<>();
                    NatsMessageProducer appOneMp = buildMessageProducer(rep2req, appOneReceived, fixture);

                    MessageHandler appTwoMh =
                            fixture.binder().createProducerMessageHandler(
                                    fixture.provisioner().provisionProducerDestination(rep2req, null), null, null);
                    CompletableFuture<org.springframework.messaging.Message<?>> appTwoReceived = new CompletableFuture<>();
                    NatsMessageProducer appTwoMp = buildMessageProducer(req2rep, appTwoReceived, fixture);
                    appTwoReceived.thenAccept(requestMsg -> appTwoMh.handleMessage(
                            MessageBuilder.withPayload(reply + " to " + payloadText(requestMsg.getPayload()))
                                    .copyHeaders(requestMsg.getHeaders())
                                    .build()));

                    try {
                        appOneMp.start();
                        appTwoMp.start();
                        fixture.connection().flush(FLUSH_TIMEOUT);

                        appOneMh.handleMessage(MessageBuilder.withPayload(request)
                                .setHeader(MessageHeaders.REPLY_CHANNEL, rep2req)
                                .build());

                        org.springframework.messaging.Message<?> appTwoMessage = appTwoReceived.get(5, TimeUnit.SECONDS);
                        assertThat(appTwoMessage.getHeaders().get(MessageHeaders.REPLY_CHANNEL)).isEqualTo(rep2req);
                        assertThat(payloadText(appTwoMessage.getPayload())).isEqualTo(request);

                        org.springframework.messaging.Message<?> appOneMessage = appOneReceived.get(5, TimeUnit.SECONDS);
                        assertThat(appOneMessage.getHeaders().get(MessageHeaders.REPLY_CHANNEL)).isEqualTo(rep2req);
                        assertThat(payloadText(appOneMessage.getPayload())).isEqualTo(reply + " to " + request);
                    } finally {
                        appOneMp.stop();
                        appTwoMp.stop();
                    }
                }
            });
        }
    }

    private static NatsMessageProducer buildCountingProducer(ConsumerDestination destination, String group,
                                                            BinderFixture fixture, AtomicInteger counter) {
        NatsMessageProducer producer =
                (NatsMessageProducer) fixture.binder().createConsumerEndpoint(destination, group, null);
        DirectChannel output = new DirectChannel();
        output.subscribe(msg -> counter.incrementAndGet());
        producer.setOutputChannel(output);
        return producer;
    }

    private static NatsMessageProducer buildMessageProducer(String subject,
                                                           CompletableFuture<org.springframework.messaging.Message<?>> received,
                                                           BinderFixture fixture) {
        NatsMessageProducer producer = (NatsMessageProducer) fixture.binder().createConsumerEndpoint(
                fixture.provisioner().provisionConsumerDestination(subject, "", null), "", null);
        DirectChannel output = new DirectChannel();
        output.subscribe(received::complete);
        producer.setOutputChannel(output);
        return producer;
    }

    private static BinderFixture newGlobalBinder(String server) throws IOException, InterruptedException {
        return newGlobalBinder(server, new String[0]);
    }

    private static BinderFixture newGlobalBinder(String server, String... headersToEmbed) throws IOException, InterruptedException {
        NatsProperties natsProperties = new NatsProperties();
        natsProperties.setServer(server);
        NatsBinderConfigurationProperties binderProperties = new NatsBinderConfigurationProperties();
        binderProperties.setHeadersToEmbed(headersToEmbed);
        return newBinder(natsProperties, binderProperties);
    }

    private static BinderFixture newBinderPropertiesBinder(String server) throws IOException, InterruptedException {
        NatsBinderConfigurationProperties binderProperties = new NatsBinderConfigurationProperties();
        binderProperties.setServer(server);
        return newBinder(new NatsProperties(), binderProperties);
    }

    private static BinderFixture newTlsGlobalBinder(String server) throws IOException, InterruptedException {
        NatsProperties natsProperties = new NatsProperties();
        natsProperties.setServer(server);
        natsProperties.setKeyStorePath(SHARED_TLS_RESOURCES + "keystore.jks");
        natsProperties.setKeyStorePassword("password".toCharArray());
        natsProperties.setKeyStoreType("JKS");
        natsProperties.setTrustStorePath(SHARED_TLS_RESOURCES + "cacerts");
        natsProperties.setTrustStorePassword("password".toCharArray());
        natsProperties.setTrustStoreType("JKS");
        return newBinder(natsProperties, new NatsBinderConfigurationProperties());
    }

    private static BinderFixture newBinder(NatsProperties natsProperties,
                                           NatsBinderConfigurationProperties binderProperties)
            throws IOException, InterruptedException {
        NatsExtendedBindingProperties props = new NatsExtendedBindingProperties();
        NatsChannelBinderConfiguration config = new NatsChannelBinderConfiguration();
        NatsChannelProvisioner provisioner = config.natsChannelProvisioner();
        config.setNatsProperties(natsProperties);
        config.setNatsBinderConfigurationProperties(binderProperties);
        config.setNatsExtendedBindingProperties(props);

        NatsChannelBinder binder = config.natsBinder(provisioner);
        assertThat(binder).isNotNull();
        assertThat(binder.getConnection()).isNotNull();
        assertThat(binder.getConnection().getStatus()).isSameAs(Connection.Status.CONNECTED);
        return new BinderFixture(provisioner, binder);
    }

    private static void assertConnected(Connection connection, String expectedUrl) {
        assertThat(connection).isNotNull();
        assertThat(connection.getStatus()).isSameAs(Connection.Status.CONNECTED);
        assertThat(connection.getConnectedUrl()).isEqualTo(expectedUrl);
    }

    private static String nextText(Subscription sub) throws InterruptedException {
        Message msg = sub.nextMessage(FLUSH_TIMEOUT);
        assertThat(msg).isNotNull();
        return new String(msg.getData(), UTF_8);
    }

    private static String payloadText(Object payload) {
        if (payload instanceof byte[] bytes) {
            return new String(bytes, UTF_8);
        }
        return payload.toString();
    }

    private static Headers headersWithTrace(String traceId) {
        return new Headers().put(TRACE_HEADER, traceId);
    }

    private static void assertDefaultListenersCanHandleCallbacks(Connection connection) {
        assertThatCode(() -> connection.getOptions().getConnectionListener()
                .connectionEvent(connection, ConnectionListener.Events.CONNECTED)).doesNotThrowAnyException();
        assertThatCode(() -> connection.getOptions().getErrorListener().slowConsumerDetected(connection, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> connection.getOptions().getErrorListener()
                .exceptionOccurred(connection, new RuntimeException("boom"))).doesNotThrowAnyException();
        assertThatCode(() -> connection.getOptions().getErrorListener().errorOccurred(connection, "boom"))
                .doesNotThrowAnyException();
    }

    private static Set<Path> tempNatsConfigs() throws IOException {
        Path tempDirectory = Path.of(System.getProperty("java.io.tmpdir"));
        try (Stream<Path> files = Files.list(tempDirectory)) {
            return files
                    .filter(path -> path.getFileName().toString().startsWith("spring_nats_test"))
                    .filter(path -> path.getFileName().toString().endsWith(".conf"))
                    .collect(Collectors.toSet());
        }
    }

    private static String missingExecutablePath() {
        return Path.of(System.getProperty("java.io.tmpdir"), "missing-nats-server-" + System.nanoTime()).toString();
    }

    private static void restoreServerPath(String previousServerPath) {
        if (previousServerPath == null) {
            System.clearProperty("nats_server_path");
        } else {
            System.setProperty("nats_server_path", previousServerPath);
        }
    }

    private record BinderFixture(NatsChannelProvisioner provisioner, NatsChannelBinder binder) implements AutoCloseable {
        private Connection connection() {
            return this.binder.getConnection();
        }

        @Override
        public void close() throws InterruptedException {
            connection().close();
        }
    }
}
