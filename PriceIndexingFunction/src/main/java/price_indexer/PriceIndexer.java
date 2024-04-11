package price_indexer;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.common.collect.Lists;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class PriceIndexer implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	private static final String TEXT_PLAIN = "text/plain";
	private static final Duration GET_PRICES_TIMEOUT = Duration.of(10, SECONDS);
	private static final Charset CHARSET = Charset.defaultCharset();
	private static final int SUCCESS_CODE = 200;
	private static final int INTERNAL_ERROR_CODE = 500;
	private static final String PRICES_TABLE_NAME = "PRICES_TABLE_NAME";
	private static final String SYMBOL_PARAM = "symbol";
	private static final int BAD_REQUEST = 400;
	private static final String CONTENT_TYPE = "Content-Type";
	private static final Logger LOGGER = LoggerFactory.getLogger(PriceIndexer.class);
	// TODO: Integrate Secrets Manager
	private static final String API_KEY = "IG9AOP32M1ZP9VBT";
	private static final int BATCH_SIZE = 25;

	private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().build();
	private final HttpClient httpClient = HttpClient.newHttpClient();

	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
		var symbol = extractSymbol(input);
		var response = new APIGatewayProxyResponseEvent();
		if (!isSymbolValid(symbol)) {
			LOGGER.warn("Received request with invalid symbol...");
			return response.withStatusCode(BAD_REQUEST).withBody("Symbol is invalid!").withHeaders(Map.of(CONTENT_TYPE, TEXT_PLAIN));
		}
		LOGGER.info("Handling request to index prices for {} symbol", symbol);
		try {
			var request = createGetPricesRequest(symbol);
			var httpResponse = httpClient.send(request, responseInfo -> HttpResponse.BodySubscribers.ofString(CHARSET));
			var body = httpResponse.body();
			if (body.contains("Invalid API call")) {
				LOGGER.error("Failed to get prices for this symbol");
				return response.withBody("Failed to get prices for this symbol").withStatusCode(INTERNAL_ERROR_CODE);
			}
			var records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new StringReader(body));
			var writeRequests = new ArrayList<WriteRequest>();
			for (var record : records) {
				var timestamp = record.get("timestamp");
				var highPrice = record.get("high");
				var lowPrice = record.get("low");
				writeRequests.add(WriteRequest.builder().putRequest(v -> v.item(Map.of(
						"Symbol", AttributeValue.fromS(symbol),
						"Timestamp", AttributeValue.fromN(String.valueOf(parseAndConvertToMillis(timestamp))),
						"LowPrice", AttributeValue.fromN(lowPrice),
						"HighPrice", AttributeValue.fromN(highPrice)
				))).build());
			}
			var pricesTableName = getPricesTableName();
			LOGGER.info("Prices table name = {}", pricesTableName);
			Lists.partition(writeRequests, BATCH_SIZE)
					.forEach(batch -> {
						var batchWriteItemResponse = dynamoDbClient.batchWriteItem(
								BatchWriteItemRequest.builder().requestItems(Map.of(pricesTableName, batch)).build());
						LOGGER.info("Batch write item response = {}", batchWriteItemResponse);
					});
			LOGGER.info("All prices for {} were written to DB!", symbol);
			return response.withStatusCode(SUCCESS_CODE).withBody("Success!").withHeaders(Map.of(CONTENT_TYPE, TEXT_PLAIN));
		}
		catch (Exception error) {
			LOGGER.warn("Error occurred on prices indexing...", error);
			return response.withBody("Error").withStatusCode(INTERNAL_ERROR_CODE);
		}
	}

	private HttpRequest createGetPricesRequest(String symbol) throws URISyntaxException {
		return HttpRequest.newBuilder()
				.uri(new URI(getPricesURL(symbol)))
				.timeout(GET_PRICES_TIMEOUT)
				.GET()
				.build();
	}

	private static String extractSymbol(APIGatewayProxyRequestEvent input) {
		return Optional.ofNullable(input.getPathParameters())
				.map(v -> v.get(SYMBOL_PARAM))
				.map(String::trim)
				.orElse(null);
	}

	private String getPricesURL(String symbol) {
		return String.format(
				"https://www.alphavantage.co/query"
						+ "?function=TIME_SERIES_DAILY"
						+ "&symbol=%s"
						+ "&datatype=csv"
						+ "&apikey=%s"
						+ "&outputsize=compact",
				symbol,
				API_KEY
		);
	}

	private static long parseAndConvertToMillis(String timestamp) {
		return LocalDate.parse(timestamp).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
	}

	private boolean isSymbolValid(String symbol) {
		return symbol != null && !symbol.isEmpty();
	}

	private String getPricesTableName() {
		return System.getenv(PRICES_TABLE_NAME);
	}
}
