package info.bitrich.xchangestream.coinbasepro.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProProductTicker;

import java.math.BigDecimal;

public class CoinbaseProStreamingProductTicker extends CoinbaseProProductTicker {
    private final String side;

    private final long sequence;

    public CoinbaseProStreamingProductTicker(@JsonProperty("trade_id") String tradeId,
                                             @JsonProperty("price") BigDecimal price,
                                             @JsonProperty("size") BigDecimal size,
                                             @JsonProperty("bid") BigDecimal bid,
                                             @JsonProperty("ask") BigDecimal ask,
                                             @JsonProperty("volume") BigDecimal volume,
                                             @JsonProperty("time") String time,
                                             @JsonProperty("side") String side,
                                             @JsonProperty("sequence") long sequence) {
        super(tradeId, price, size, bid, ask, volume, time);
        this.side = side;
        this.sequence = sequence;
    }

    public String getSide() {
        return side;
    }

    public long getSequence() {
        return sequence;
    }
}
