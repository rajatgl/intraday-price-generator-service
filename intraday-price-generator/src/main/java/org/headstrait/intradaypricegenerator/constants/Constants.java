package org.headstrait.intradaypricegenerator.constants;


public class Constants {

    private Constants() {
    }

    //ohlc folder path
    private static final String OHLC_FOLDER_PATH_STRING = "intraday-price-generator\\src\\main\\resources\\ohlc";

    //tickers of interest
    private static final String[] STOCKS = new String[]{"asfa.ns","reliance.ns"};

    //exchanges
    private static final String[] EXCHANGES = new String[]{"BSE","NSE"};

    //exchange ratio
    private static final Long[] EXCHANGE_PRODUCTION_RATIO = new Long[]{1L, 2L};

    public static final String STOCK_SIMULATOR_BOOTSTRAP_BROKER = System.getenv("STOCK_SIMULATOR_BOOTSTRAP_BROKER");

    public static final String REQUEST_SIZE = "10000000";

    public static String getOhlcFolderPathString() {
        return OHLC_FOLDER_PATH_STRING;
    }

    public static String[] getStocks() {
        return STOCKS;
    }

    public static String[] getExchanges() {
        return EXCHANGES;
    }

    public static Long[] getExchangeProductionRatio() {
        return EXCHANGE_PRODUCTION_RATIO;
    }
}
