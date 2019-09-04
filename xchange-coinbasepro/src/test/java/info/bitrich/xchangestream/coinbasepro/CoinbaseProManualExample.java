package info.bitrich.xchangestream.coinbasepro;

import io.reactivex.functions.Consumer;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

public class CoinbaseProManualExample {
    private static final Logger LOG = LoggerFactory.getLogger(CoinbaseProManualExample.class);

    public static void main(String[] args) {
        ProductSubscription productSubscription = ProductSubscription.create().addAll(CurrencyPair.BTC_USD)
                .addAll(CurrencyPair.BTC_EUR).addTicker(CurrencyPair.ETH_USD).build();

        ExchangeSpecification specification = new CoinbaseProStreamingExchange().getDefaultExchangeSpecification();
        specification.setExchangeSpecificParametersItem(StreamingExchange.ENABLE_LOGGING_HANDLER, Boolean.valueOf(true));
        StreamingExchange exchange = StreamingExchangeFactory.INSTANCE.createExchange(specification);

        exchange.connect(productSubscription).blockingAwait();

        /**
        exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.BTC_USD).subscribe(orderBook -> {
            LOG.info("First ask: {}", orderBook.getAsks().get(0));
            LOG.info("First bid: {}", orderBook.getBids().get(0));
        }, throwable -> LOG.error("ERROR in getting order book: ", throwable));

        exchange.getStreamingMarketDataService().getTicker(CurrencyPair.ETH_USD).subscribe(ticker -> {
            LOG.info("TICKER: {}", ticker);
        }, throwable -> LOG.error("ERROR in getting ticker: ", throwable));

        exchange.getStreamingMarketDataService().getTrades(CurrencyPair.BTC_USD)
                .subscribe(trade -> {
                    LOG.info("TRADE: {}", trade);
                }, throwable -> LOG.error("ERROR in getting trades: ", throwable));
*/

        AtomicReference<LocalDateTime> lastRate = new AtomicReference<>(null);
        CoinbaseProStreamingMarketDataService service = (CoinbaseProStreamingMarketDataService)exchange.getStreamingMarketDataService();

        final Consumer<OrderBook> orderBookConsumer = (orderBook) -> {
            lastRate.set(LocalDateTime.now());
        };

        service.getOrderBook(CurrencyPair.BTC_USD).subscribe(orderBookConsumer);

        while (lastRate.get() == null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        exchange.disconnect().blockingAwait();
        exchange.connect(productSubscription).blockingAwait();
        LocalDateTime previousTime = lastRate.get();

        service = (CoinbaseProStreamingMarketDataService)exchange.getStreamingMarketDataService();
        service.getOrderBook(CurrencyPair.BTC_USD).subscribe(orderBookConsumer);

        while (previousTime.equals(lastRate.get())) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println(previousTime + " now " + lastRate.get());


        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        exchange.disconnect().blockingAwait();
    }
}
