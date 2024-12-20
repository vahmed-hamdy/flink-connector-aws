package org.apache.flink.connector.sqs.sql.sink.test;

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.sqs.sink.test.SqsSinkITTest;
import org.apache.flink.connector.sqs.sink.testutils.SqsTestUtils;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.DockerImageVersions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.connector.sqs.sink.testutils.SqsTestUtils.createSqsClient;

/** Integration tests for the SQS connector. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class SqsTableIT {
    private static final Logger LOG = LoggerFactory.getLogger(SqsSinkITTest.class);

    private static final int NUMBER_OF_ELEMENTS = 10;
    private SdkHttpClient httpClient;
    private SqsClient sqsClient;
    private static final Network network = Network.newNetwork();

    private final Path sqlConnectorSqsJar =
            ResourceTestUtils.getResource(".*/flink-sql-connector-sqs[^/]*\\.jar");

    @ClassRule
    public static LocalstackContainer mockSqsContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK))
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .environmentVariable("AWS_CBOR_DISABLE", "1")
                    .environmentVariable(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.sqs.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .network(network)
                    .logger(LOG)
                    .dependsOn(mockSqsContainer)
                    .build();

    public static final FlinkContainers FLINK =
            FlinkContainers.builder().withTestcontainersSettings(TESTCONTAINERS_SETTINGS).build();

    @Before
    public void setup() throws Exception {
        httpClient = AWSServicesTestUtils.createHttpClient();
        sqsClient = createSqsClient(mockSqsContainer.getEndpoint(), httpClient);
        LOG.info("Done setting up the localstack.");
    }

    @BeforeClass
    public static void setupFlink() throws Exception {
        FLINK.start();
    }

    @AfterClass
    public static void stopFlink() {
        FLINK.stop();
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        httpClient.close();
        sqsClient.close();
    }

    @Test
    public void sqsTableApiWritesDataToLocalStack() throws Exception {
        SqsTestUtils.createSqs("test-sqs", sqsClient);
        List<String> sqlLines = readSqlFile("send-orders.sql");
        executeSqlStatements(sqlLines);
        List<Order> expectedOrders =
                Arrays.asList(
                        new Order("order1", 1),
                        new Order("order2", 2),
                        new Order("order3", 3),
                        new Order("order4", 4),
                        new Order("order5", 5));

        ReceiveMessageRequest receiveMessageRequest =
                ReceiveMessageRequest.builder()
                        .queueUrl("http://localhost:4576/queue/test-sqs")
                        .maxNumberOfMessages(5)
                        .build();

        List<Message> messages =
                new ArrayList<>(sqsClient.receiveMessage(receiveMessageRequest).messages());

        ObjectMapper objectMapper = createObjectMapper();
        List<Order> actualOrders =
                messages.stream()
                        .map(
                                order -> {
                                    try {
                                        return objectMapper.readValue(order.body(), Order.class);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toList());

        Assertions.assertThat(actualOrders).containsExactlyInAnyOrderElementsOf(expectedOrders);
    }

    private List<String> readSqlFile(final String resourceName)
            throws IOException, URISyntaxException {
        return Files.readAllLines(Paths.get(getClass().getResource("/" + resourceName).toURI()));
    }

    private void executeSqlStatements(final List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorSqsJar)
                        .build());
    }

    /** POJO class for orders used by e2e test. */
    public static class Order {
        private final String code;
        private final int quantity;

        public Order(
                @JsonProperty("code") final String code, @JsonProperty("quantity") int quantity) {
            this.code = code;
            this.quantity = quantity;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Order order = (Order) o;
            return quantity == order.quantity && Objects.equals(code, order.code);
        }

        public String getCode() {
            return code;
        }

        public int getQuantity() {
            return quantity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, quantity);
        }

        @Override
        public String toString() {
            return String.format("Order{code: %s, quantity: %d}", code, quantity);
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        registerModules(objectMapper);
        return objectMapper;
    }

    private static void registerModules(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
