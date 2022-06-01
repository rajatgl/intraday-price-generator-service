package org.headstrait.intradaypricegeneratortest.constants;

import org.headstrait.intradaypricegenerator.model.IntradayPrice;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.model.Ohlc;
import org.headstrait.intradaypricegenerator.model.S3UploaderRequestEventDTO;

import java.util.List;
import java.util.UUID;

public class TestObjects {
    public static final IntradayPrice INTRADAY_PRICE1 = IntradayPrice.builder()
            .stockSymbol("stock.ns")
            .date("05/06/2023")
            .time("9:15:00.000000000")
            .bidPrice(1001d)
            .bidExchange("N")
            .bidSize(1500L)
            .askPrice(1000d)
            .askExchange("N")
            .askSize(1500L).build();

    public static final List<IntradayPrice> INTRADAY_PRICES = List.of(INTRADAY_PRICE1);
    public static final UUID REQUEST_ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    public static final List<String> HEADERS = List.of("stockSymbol","date","time","bidPrice",
            "bidExchange","bidSize","askPrice","askSize","askExchange");

    public static final CsvGeneratorEventDTO CSV_GENERATOR_EVENT_DTO = CsvGeneratorEventDTO.builder()
            .requestId(REQUEST_ID)
            .headers(HEADERS)
            .data(INTRADAY_PRICES.toString())
            .numberOfRows(INTRADAY_PRICES.size())
            .fileName("stock-ns-intraday-05-June-2023.csv")
            .build();

    public static final S3UploaderRequestEventDTO S3_UPLOADER_REQUEST_DTO = S3UploaderRequestEventDTO.builder()
            .requestId(REQUEST_ID)
            .headers(HEADERS)
            .data(INTRADAY_PRICES.toString())
            .numberOfRows(INTRADAY_PRICES.size())
            .fileName("stock-ns-intraday-05-June-2023.csv")
            .s3FilePath("05-June-2023/STOCK")
            .bucketName("s3-uploader-bucket-test-1")
            .build();

    public static final String CSV_FILE_CONTENT1 = "CSV_FILE_1_CONTENT";
    public static final String CSV_FILE_CONTENT2 = "CSV_FILE_2_CONTENT";
    public static final List<String> FILE_CONTENTS = List.of(CSV_FILE_CONTENT1,CSV_FILE_CONTENT2);

    public static final Ohlc OHLC = new Ohlc(
            "2023-06-05",
            1000d, 1043d, 963d,
            1030d, 1030d, 25020000L);

    public static final List<Ohlc> OHLC_LIST = List.of(OHLC);

    public static final String OHLC_FOLDER_PATH_STRING = "intraday-price-generator\\src\\test\\resources\\ohlc";
}
