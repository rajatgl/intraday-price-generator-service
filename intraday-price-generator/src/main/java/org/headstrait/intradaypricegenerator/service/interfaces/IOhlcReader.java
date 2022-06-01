package org.headstrait.intradaypricegenerator.service.interfaces;

import org.headstrait.intradaypricegenerator.model.Ohlc;

import java.util.List;

public interface IOhlcReader {
    /**
     *
     * @param stock stock symbol of stock whose ohlc data is to be fetched
     * @return list of ohlc objects for various dates
     */
    List<Ohlc> getOhlcDataForAStock(String stock);
}
