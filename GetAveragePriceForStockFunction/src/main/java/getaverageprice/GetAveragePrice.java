package getaverageprice;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class GetAveragePrice implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAveragePrice.class);

    private static final String PRICES_TABLE_NAME = "PRICES_TABLE_NAME";
    private static final String SYMBOL_PARAM = "symbol";
    private static final String FROM_PARAM = "from";
    private static final String TO_PARAM = "to";
    private static final int BAD_REQUEST = 400;
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String TEXT_PLAIN = "text/plain";
    private static final int LIMIT = 100;
    private static final String LOW_PRICE = "LowPrice";
    private static final String HIGH_PRICE = "HighPrice";
    private static final int SUCCESS = 200;
    private static final int SERVER_ERROR = 500;
    private static final MathContext MATH_CONTEXT = new MathContext(2);

    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent apiGatewayProxyRequestEvent, Context context) {
        var symbol = getPathParam(apiGatewayProxyRequestEvent, SYMBOL_PARAM);
        var from = getPathParam(apiGatewayProxyRequestEvent, FROM_PARAM);
        var to = getPathParam(apiGatewayProxyRequestEvent, TO_PARAM);
        var response = new APIGatewayProxyResponseEvent();
        if (symbol == null) {
            return createBadRequest(response, "Symbol was not provided!");
        }
        if (from == null) {
            return createBadRequest(response, "From was not provided!");
        }
        if (to == null) {
            return createBadRequest(response, "To was not provided!");
        }
        try {
            var tableName = getPricesTableName();
            LOGGER.info("Getting prices statistics for {} between [{}, {}] from table {}", symbol, from, to, tableName);
            var queryResponse = dynamoDbClient.query(b -> b
                    .tableName(tableName)
                    .keyConditionExpression("Symbol = :symbol AND #timestamp BETWEEN :from AND :to")
                    .expressionAttributeNames(Map.of(
                            "#timestamp", "Timestamp"
                    ))
                    .expressionAttributeValues(Map.of(
                            ":symbol", AttributeValue.builder().s(symbol).build(),
                            ":from", AttributeValue.builder().n(from).build(),
                            ":to", AttributeValue.builder().n(to).build()
                    ))
                    .projectionExpression(LOW_PRICE + "," + HIGH_PRICE)
                    .limit(LIMIT)
            );
            var responseItems = queryResponse.items();
            int numItems = responseItems.size();
            if (numItems == 0) {
                LOGGER.info("Data not found...");
                return response.withStatusCode(SUCCESS)
                        .withBody("No data...")
                        .withHeaders(Map.of(CONTENT_TYPE, TEXT_PLAIN));
            }
            var sumLower = sumOfPrices(responseItems, LOW_PRICE);
            var sumHigher = sumOfPrices(responseItems, HIGH_PRICE);
            var minLower = minPrice(responseItems, LOW_PRICE, BigDecimal::compareTo);
            var minHigh = minPrice(responseItems, HIGH_PRICE, BigDecimal::compareTo);
            var maxLower = minPrice(responseItems, LOW_PRICE, Comparator.reverseOrder());
            var maxHigh = minPrice(responseItems, HIGH_PRICE, Comparator.reverseOrder());
            var lowerMedian = median(extractPrices(responseItems, LOW_PRICE));
            var higherMedian = median(extractPrices(responseItems, HIGH_PRICE));
            return response.withStatusCode(SUCCESS)
                    .withBody(format(Map.of(
                            "Average lower", (sumLower.divide(BigDecimal.valueOf(numItems), MATH_CONTEXT)),
                            "Average higher", (sumHigher.divide(BigDecimal.valueOf(numItems), MATH_CONTEXT)),
                            "Prices analyzed", BigDecimal.valueOf(numItems),
                            "Min lower", minLower,
                            "Min higher", minHigh,
                            "Max lower", maxLower,
                            "Max higher", maxHigh,
                            "Lower median", lowerMedian,
                            "Higher median", higherMedian
                    )))
                    .withHeaders(Map.of(CONTENT_TYPE, TEXT_PLAIN));
        }
        catch (Exception error) {
            LOGGER.error("Error occurred on fetch of statistics", error);
            return response.withBody("Server error").withStatusCode(SERVER_ERROR);
        }
    }

    private BigDecimal median(List<BigDecimal> list) {
        var copy = new ArrayList<>(list);
        copy.sort(BigDecimal::compareTo);
        var n = copy.size();
        return n % 2 == 0 ? (copy.get(n / 2).add(copy.get(n / 2 - 1)).divide(BigDecimal.TWO, MATH_CONTEXT)) : copy.get(n / 2);
    }

    private String format(Map<String, BigDecimal> params) {
        return params.entrySet().stream().map(v -> v.getKey() + " = " + v.getValue()).collect(Collectors.joining(", "));
    }

    private List<BigDecimal> extractPrices(List<Map<String, AttributeValue>> responseItems, String fieldName) {
        return responseItems.stream()
                .map(v -> v.get(fieldName))
                .map(v -> new BigDecimal(v.n()))
                .toList();
    }

    private static BigDecimal sumOfPrices(List<Map<String, AttributeValue>> responseItems, String fieldName) {
        return responseItems.stream()
                .map(v -> v.get(fieldName))
                .map(v -> new BigDecimal(v.n()))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private static BigDecimal minPrice(List<Map<String, AttributeValue>> responseItems, String fieldName, Comparator<BigDecimal> comparator) {
        return responseItems.stream()
                .map(v -> v.get(fieldName))
                .map(v -> new BigDecimal(v.n()))
                .min(comparator)
                .orElseThrow();
    }

    private static APIGatewayProxyResponseEvent createBadRequest(APIGatewayProxyResponseEvent response, String reason) {
        LOGGER.warn(reason);
        return response.withStatusCode(BAD_REQUEST)
                .withBody(reason)
                .withHeaders(Map.of(CONTENT_TYPE, TEXT_PLAIN));
    }

    private String getPricesTableName() {
        return System.getenv(PRICES_TABLE_NAME);
    }

    private static String getPathParam(APIGatewayProxyRequestEvent input, String paramName) {
        return Optional.ofNullable(input.getPathParameters())
                .map(v -> v.get(paramName))
                .map(String::trim)
                .orElse(null);
    }

}
