package org.headstrait.intradaypricegenerator.service;

import org.headstrait.intradaypricegenerator.model.IntradayPrice;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.model.Ohlc;
import org.headstrait.intradaypricegenerator.service.interfaces.IOhlcReader;
import org.headstrait.intradaypricegenerator.service.interfaces.IProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IntradayPricesEventProducer {
    private final IOhlcReader ohlcReader;

    private final IProducer<String,CsvGeneratorEventDTO> producer;

    private final IntradayPriceGenerator intradayPriceGenerator;

    private List<Ohlc> ohlcs;

    private final Logger logger = LoggerFactory.getLogger(IntradayPricesEventProducer.class);

    private final SimpleDateFormat sourceDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat destinationDateFormat = new SimpleDateFormat("dd-MMMMM-yyyy");


    public IntradayPricesEventProducer(IOhlcReader ohlcReader, IProducer<String,CsvGeneratorEventDTO> producer, IntradayPriceGenerator intradayPriceGenerator) {
        this.ohlcReader = ohlcReader;
        this.producer = producer;
        this.intradayPriceGenerator = intradayPriceGenerator;
        ohlcs = new ArrayList<>();
    }


    /**
     * this method produces price events for each day mentioned in the ohlc file for a stock
     *
     * @param stock whose price ranges for each day need to be produced.
     * @throws IOException
     */
    public void produce(String stock) throws ParseException {
        //first step is to read the ohlc content for the given stock
        ohlcs = ohlcReader.getOhlcDataForAStock(stock);

        //if no content found, return off the method.
        if (ohlcs.isEmpty()) {
            logger.warn("No ohlc data found for the stock: {}", stock);
            logger.warn("EXITING THE LOOP .....");
            return;
        }

        //initially isAlternating is set to true so price wave will hit high first.
        boolean isAlternating = true;

        //for each ohlc read by the ohlcReader generate and send the price range.
        for (Ohlc ohlc :
                ohlcs) {
            //generate the price range
            List<IntradayPrice> intradayPriceList = intradayPriceGenerator.getRandomPrices(stock, ohlc,isAlternating);
            logger.info("INTRADAY PRICE RANGE SIZE: {}",intradayPriceList.size());
            String data = intradayPriceList.toString();
            UUID uuid = UUID.randomUUID();
            List<String> headers = List.of("stockSymbol","date","time","bidPrice",
                    "bidExchange","bidSize","askPrice","askSize","askExchange");
            Long timeStamp = 0L;
            try {
                timeStamp = sourceDateFormat.parse(ohlc.getDate()).getTime();
            }catch (Exception exception){
                exception.printStackTrace();
            }

            String[] stockSplit = stock.split("\\.");
            String formattedDate = destinationDateFormat.format(timeStamp);
            String fileName =stockSplit[0]+"-"+stockSplit[1]+"-"+"intraday"+"-"+formattedDate+".csv";
            //wrap the price range within eventDTO
            CsvGeneratorEventDTO csvGeneratorEventDTO = CsvGeneratorEventDTO.builder()
                    .fileName(fileName)
                    .requestId(uuid)
                    .headers(headers)
                    .data(data)
                    .numberOfRows(intradayPriceList.size())
                    .build();
            logger.info("SENDING EVENT WITH REQUEST_ID: {}", csvGeneratorEventDTO.getRequestId());
            logger.info("FILENAME: {}",fileName);
            //send the event with ohlc date as key and eventDto as value.
            producer.send(ohlc.getDate(), csvGeneratorEventDTO);

            //reverse the isAlternating value.
            isAlternating = !isAlternating;
        }
    }
}
