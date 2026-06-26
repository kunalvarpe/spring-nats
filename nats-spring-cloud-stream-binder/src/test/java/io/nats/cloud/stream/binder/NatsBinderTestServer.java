// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.cloud.stream.binder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs a real nats-server for binder tests.
 */
public class NatsBinderTestServer implements AutoCloseable {

    private static final String NATS_SERVER = "nats-server";
    private static final Duration START_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);
    private static final Pattern PORT_PATTERN = Pattern.compile("(^\\s*port\\s*:\\s*)\\d+(.*$)");

    private static final AtomicInteger portCounter = new AtomicInteger(11223);

    private final int port;
    private final boolean debug;
    private final String configFilePath;
    private final String[] customArgs;
    private final String[] configInserts;
    private Process process;
    private String cmdLine;
    private Path generatedConfig;

    public NatsBinderTestServer() {
        this(false);
    }

    public NatsBinderTestServer(boolean debug) {
        this(nextPort(), debug);
    }

    public NatsBinderTestServer(int port, boolean debug) {
        this(port, debug, null, null, null);
    }

    private NatsBinderTestServer(int port, boolean debug, String configFilePath, String[] customArgs, String[] configInserts) {
        this.port = port;
        this.debug = debug;
        this.configFilePath = configFilePath;
        this.customArgs = customArgs;
        this.configInserts = configInserts;
        start();
    }

    public NatsBinderTestServer(String configFilePath, boolean debug) {
        this(nextPort(), debug, configFilePath, null, null);
    }

    public NatsBinderTestServer(String configFilePath, String[] inserts, int port, boolean debug) {
        this(port, debug, configFilePath, null, inserts);
    }

    public NatsBinderTestServer(String configFilePath, int port, boolean debug) {
        this(port, debug, configFilePath, null, null);
    }

    public NatsBinderTestServer(String[] customArgs, boolean debug) {
        this(nextPort(), debug, null, customArgs, null);
    }

    public NatsBinderTestServer(String[] customArgs, int port, boolean debug) {
        this(port, debug, null, customArgs, null);
    }

    public static String generateNatsServerVersionString() {
        ArrayList<String> cmd = new ArrayList<>();
        cmd.add(serverPath());
        cmd.add("--version");

        try {
            Process process = new ProcessBuilder(cmd).start();
            if (!process.waitFor(START_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return "";
            }

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                return line == null ? "" : line;
            }
        } catch (Exception exp) {
            return "";
        }
    }

    public static int nextPort() {
        return NatsBinderTestServer.portCounter.incrementAndGet();
    }

    public static int currentPort() {
        return NatsBinderTestServer.portCounter.get();
    }

    public static String getURIForPort(int port) {
        return "nats://localhost:" + port;
    }

    private static String serverPath() {
        String serverPath = System.getProperty("nats_server_path");
        if (!hasText(serverPath)) {
            serverPath = System.getenv("nats_server_path");
        }
        return hasText(serverPath) ? serverPath : NATS_SERVER;
    }

    private void start() {
        ArrayList<String> cmd = new ArrayList<>();
        cmd.add(serverPath());

        if (this.configFilePath != null) {
            Path config = rewriteConfig();
            cmd.add("--config");
            cmd.add(config.toAbsolutePath().toString());
        } else {
            cmd.add("--port");
            cmd.add(String.valueOf(this.port));
        }

        if (this.customArgs != null) {
            cmd.addAll(Arrays.asList(this.customArgs));
        }

        if (this.debug) {
            cmd.add("-DV");
        }

        this.cmdLine = String.join(" ", cmd);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);

            if (this.debug) {
                System.out.println("%%% Starting [" + this.cmdLine + "] with redirected IO");
                pb.inheritIO();
            } else {
                pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
                pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            }

            this.process = pb.start();
            waitForReady();
            System.out.println("%%% Started [" + this.cmdLine + "]");
        } catch (IOException ex) {
            deleteGeneratedConfig();
            System.out.println("%%% Failed to start [" + this.cmdLine + "] with message:");
            System.out.println("\t" + ex.getMessage());
            System.out.println("%%% Make sure that the nats-server is installed and in your PATH.");
            System.out.println("%%% See https://github.com/nats-io/nats-server for information on installation");

            throw new IllegalStateException("Failed to run [" + this.cmdLine + "]", ex);
        } catch (RuntimeException ex) {
            shutdown();
            throw ex;
        }
    }

    private Path rewriteConfig() {
        try {
            Path source = Path.of(this.configFilePath).toAbsolutePath().normalize();
            Path target = Files.createTempFile("spring_nats_test", ".conf");
            this.generatedConfig = target;

            List<String> lines = new ArrayList<>();
            String resourceRoot = source.getParent().toString().replace(File.separatorChar, '/') + "/";

            for (String originalLine : Files.readAllLines(source, StandardCharsets.UTF_8)) {
                Matcher matcher = PORT_PATTERN.matcher(originalLine);
                String line = matcher.matches() ? matcher.group(1) + this.port + matcher.group(2) : originalLine;
                lines.add(line.replace("src/test/resources/", resourceRoot));
            }

            if (this.configInserts != null) {
                lines.addAll(Arrays.asList(this.configInserts));
            }

            Files.write(target, lines, StandardCharsets.UTF_8);
            return target;
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to rewrite NATS config " + this.configFilePath, ex);
        }
    }

    private void waitForReady() {
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        IOException lastFailure = null;

        while (System.nanoTime() < deadline) {
            if (this.process == null || !this.process.isAlive()) {
                throw new IllegalStateException("nats-server exited before accepting connections [" + this.cmdLine + "]");
            }

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", this.port), 250);
                socket.setSoTimeout(250);
                String infoLine = readInfoLine(socket);
                if (infoLine != null && infoLine.startsWith("INFO ")) {
                    if (serverExitedAfterReadyProbe()) {
                        throw new IllegalStateException("nats-server exited before accepting connections [" + this.cmdLine + "]");
                    }
                    return;
                }
            } catch (IOException ex) {
                lastFailure = ex;
                sleepBeforeRetry();
            }
        }

        throw new IllegalStateException("Timed out waiting for nats-server on port " + this.port + " [" + this.cmdLine + "]",
                lastFailure);
    }

    private String readInfoLine(Socket socket) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        return reader.readLine();
    }

    private boolean serverExitedAfterReadyProbe() {
        try {
            return this.process.waitFor(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted checking nats-server readiness [" + this.cmdLine + "]", ex);
        }
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for nats-server [" + this.cmdLine + "]", ex);
        }
    }

    public int getPort() {
        return this.port;
    }

    public String getURI() {
        return getURIForPort(this.port);
    }

    public NatsBinderTestServer shutdown() {
        if (this.process != null) {
            this.process.destroy();
            try {
                if (!this.process.waitFor(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    this.process.destroyForcibly();
                    this.process.waitFor(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                this.process.destroyForcibly();
            }
            System.out.println("%%% Shut down [" + this.cmdLine + "]");
        }

        this.process = null;
        deleteGeneratedConfig();
        return this;
    }

    private void deleteGeneratedConfig() {
        if (this.generatedConfig == null) {
            return;
        }
        try {
            Files.deleteIfExists(this.generatedConfig);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to delete temporary NATS config " + this.generatedConfig, ex);
        } finally {
            this.generatedConfig = null;
        }
    }

    private static boolean hasText(String text) {
        return text != null && !text.isBlank();
    }

    /**
     * Synonymous with shutdown.
     */
    @Override
    public void close() {
        shutdown();
    }
}
