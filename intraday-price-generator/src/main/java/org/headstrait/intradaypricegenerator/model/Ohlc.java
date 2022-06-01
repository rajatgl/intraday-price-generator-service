package org.headstrait.intradaypricegenerator.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


//ohlc model read from the csv folder
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Ohlc implements Serializable {
    private String date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double adjClose;
    private Long volume;
}
