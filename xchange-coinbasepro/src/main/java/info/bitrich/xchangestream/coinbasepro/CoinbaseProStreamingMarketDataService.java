package info.bitrich.xchangestream.coinbasepro;

import static io.netty.util.internal.StringUtil.isNullOrEmpty;
import static org.knowm.xchange.coinbasepro.CoinbaseProAdapters.adaptOrderBook;
import static org.knowm.xchange.coinbasepro.CoinbaseProAdapters.adaptTicker;
import static org.knowm.xchange.coinbasepro.CoinbaseProAdapters.adaptTradeHistory;
import static org.knowm.xchange.coinbasepro.CoinbaseProAdapters.adaptTrades;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import info.bitrich.xchangestream.coinbasepro.dto.CoinbaseProStreamingProductTicker;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProProductBook;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProProductTicker;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProTrade;
import org.knowm.xchange.coinbasepro.dto.trade.CoinbaseProFill;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;

import com.fasterxml.jackson.databind.ObjectMapper;

import info.bitrich.xchangestream.coinbasepro.dto.CoinbaseProWebSocketTransaction;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.Observable;

/**
 * Created by luca on 4/3/17.
 */
public class CoinbaseProStreamingMarketDataService implements StreamingMarketDataService {

    private final CoinbaseProStreamingService service;
    private final Map<CurrencyPair, SortedMap<BigDecimal, String>> bids = new HashMap<>();
    private final Map<CurrencyPair, SortedMap<BigDecimal, String>> asks = new HashMap<>();

    CoinbaseProStreamingMarketDataService(CoinbaseProStreamingService service) {
        this.service = service;
    }

    private boolean containsPair(List<CurrencyPair> pairs, CurrencyPair pair) {
        for (CurrencyPair item : pairs) {
            if (item.compareTo(pair) == 0) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        if (!containsPair(service.getProduct().getOrderBook(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for orderbook", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();

        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        final int maxDepth = (args.length > 0 && args[0] instanceof Integer) ? (int) args[0] : 100;

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) &&
                        (message.getType().equals("snapshot") || message.getType().equals("l2update")) &&
                        message.getProductId().equals(channelName))
                .map(s -> {
                    if (s.getType().equals("snapshot")) {
                        bids.put(currencyPair, new TreeMap<>(java.util.Collections.reverseOrder()));
                        asks.put(currencyPair, new TreeMap<>());
                    }

                    CoinbaseProProductBook productBook = s.toCoinbaseProProductBook(bids.get(currencyPair), asks.get(currencyPair), maxDepth);

                    Date date = null;
                    if (s.getTime() != null) {
                        date = Date.from(Instant.parse(s.getTime()));
                    }

                    return adaptOrderBook(productBook, currencyPair, date);
                });
    }

    /**
     * Returns an Observable of {@link CoinbaseProProductTicker}, not converted to {@link Ticker}
     *
     * @param currencyPair the currency pair.
     * @param args         optional arguments.
     * @return an Observable of {@link CoinbaseProProductTicker}.
     */
    public Observable<CoinbaseProStreamingProductTicker> getRawTicker(CurrencyPair currencyPair, Object... args) {
        return rawTicker(currencyPair).map(CoinbaseProWebSocketTransaction::toCoinbaseProProductTicker);
    }

    private Observable<CoinbaseProWebSocketTransaction> rawTicker(CurrencyPair currencyPair) {
        if (!containsPair(service.getProduct().getTicker(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for ticker", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();

        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) && message.getType().equals("ticker") &&
                        message.getProductId().equals(channelName));
    }

    /**
     * Returns the CoinbasePro ticker converted to the normalized XChange object.
     * CoinbasePro does not directly provide ticker data via web service.
     * As stated by: https://docs.coinbasepro.com/#get-product-ticker, we can just listen for 'match' messages.
     *
     * @param currencyPair Currency pair of the ticker
     * @param args         optional parameters.
     * @return an Observable of normalized Ticker objects.
     */
    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        return rawTicker(currencyPair).map(s ->
                adaptTicker(s.toCoinbaseProProductTicker(), s.toCoinbaseProProductStats(), currencyPair));
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        if (!containsPair(service.getProduct().getTrades(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for trades", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();

        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) && message.getType().equals("match") &&
                        message.getProductId().equals(channelName))
                .map(s -> {
                            Trades adaptedTrades = null;
                            if ( s.getUserId() != null )
                                adaptedTrades = adaptTradeHistory(new CoinbaseProFill[]{s.toCoinbaseProFill()});
                            else
                                adaptedTrades = adaptTrades(new CoinbaseProTrade[]{s.toCoinbaseProTrade()}, currencyPair);
                            return adaptedTrades.getTrades().get(0);
                        }
                );
    }
}
