package org.headstrait.intradaypricegenerator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IntradayPrice implements Serializable {
    private String stockSymbol;
    private String date;
    private String time;
    private Double bidPrice;
    private String bidExchange;
    private Long bidSize;
    private Double askPrice;
    private String askExchange;
    private Long askSize;

    @Override
    public String toString() {
        return '{' +
                "stockSymbol:'" + stockSymbol + '\'' +
                ", date:'" + date + '\'' +
                ", time:'" + time + '\'' +
                ", bidPrice:" + bidPrice +
                ", bidExchange:'" + bidExchange + '\'' +
                ", bidSize:" + bidSize +
                ", askPrice:" + askPrice +
                ", askExchange:'" + askExchange + '\'' +
                ", askSize:" + askSize +
                '}';
    }
}
