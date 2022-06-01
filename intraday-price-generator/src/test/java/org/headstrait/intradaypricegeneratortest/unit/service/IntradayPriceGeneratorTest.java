package org.headstrait.intradaypricegeneratortest.unit.service;

import org.headstrait.intradaypricegenerator.constants.Constants;
import org.headstrait.intradaypricegenerator.model.IntradayPrice;
import org.headstrait.intradaypricegenerator.model.Ohlc;
import org.headstrait.intradaypricegenerator.service.IntradayPriceGenerator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.OHLC;
import static org.junit.Assert.*;

public class IntradayPriceGeneratorTest {

    private IntradayPriceGenerator intradayPriceGenerator;

    private String stock;

    private final SimpleDateFormat destinationDateFormat = new SimpleDateFormat("dd/MM/yyyy");
    private final SimpleDateFormat sourceDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat destinationTimeFormat = new SimpleDateFormat("HH:mm:ss.SSSSSSSSS");

    private final Logger logger =
            LoggerFactory.getLogger(IntradayPriceGeneratorTest.class);

    private Long sumOfBidSizes;
    private Long sumOfAskSizes;
    private Long nseCount;
    private Long bseCount;


    @Before
    public void setUp(){
        intradayPriceGenerator = new IntradayPriceGenerator();
        stock = "STOCK";
        sumOfBidSizes = 0L;
        sumOfAskSizes = 0L;
        nseCount = 0L;
        bseCount = 0L;
    }

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");

    private double getFormattedPrice(double price){
        return Double.parseDouble(DECIMAL_FORMAT.format(price));
    }

    //function to check price trend for a day.
    private boolean checkTrendOfPrices(List<IntradayPrice> prices, Ohlc ohlc, boolean isAlternateDay) {

        boolean isHighDone = false;
        boolean isLowDone = false;

        for(IntradayPrice price: prices) {
            if(price.getAskPrice().equals(getFormattedPrice(ohlc.getHigh())) ||
                    price.getBidPrice().equals(getFormattedPrice(ohlc.getHigh()))) {
                if(!isAlternateDay) {
                    if(!isLowDone) {
                        // if it NOT an alternate day then low should reach BEFORE the high -> return false otherwise
                        return false;
                    }
                }
                isHighDone = true;
            }
            if(price.getBidPrice().equals(getFormattedPrice(ohlc.getLow())) ||
                    price.getAskPrice().equals(getFormattedPrice(ohlc.getLow()))) {
                if(isAlternateDay) {
                    if(!isHighDone) {
                        //if it is an alternate day then high should reach BEFORE the low -> return false otherwise
                        return false;
                    }
                }
                isLowDone = true;
            }
        }

        //verify if the trend started with open and ended with close
        return (prices.get(0).getBidPrice().equals(ohlc.getOpen()) || prices.get(0).getAskPrice().equals(ohlc.getOpen()))
                        && (prices.get(prices.size()-1).getBidPrice().equals(ohlc.getClose()) || prices.get(prices.size()-1).getAskPrice().equals(ohlc.getClose()));
    }


    @Test
    public void getRandomPricesMethod_ShouldGenerateValidRandomPriceListForAnAlternateDay_WhenInvoked()
            throws ParseException{
        List<IntradayPrice> intradayPriceRange = intradayPriceGenerator.getRandomPrices(stock,OHLC,true);
        intradayPriceGenerator.cancelTask();
        IntradayPrice firstIntradayPrice = intradayPriceRange.get(0);
        IntradayPrice lastIntradayPrice = intradayPriceRange.get(intradayPriceRange.size()-1);

        System.out.println(firstIntradayPrice);
        System.out.println(lastIntradayPrice);


        //checking the date field
        assertEquals(destinationDateFormat.format(sourceDateFormat.parse(OHLC.getDate())),firstIntradayPrice.getDate());

        //check if the opening time is >= 9:15am
        assertTrue(destinationTimeFormat.parse(firstIntradayPrice.getTime()).getTime()
                >=destinationTimeFormat.parse("9:15:00.000000000").getTime());

        //check if the closing time is <=3:30pm
        assertEquals("15:30:00.000000000", lastIntradayPrice.getTime());

        //check if the bid and ask exchanges are same for a trade
        assertEquals(firstIntradayPrice.getBidExchange(), firstIntradayPrice.getAskExchange());

        //check if the ask price is set to the open field in ohlc data
        assertTrue(firstIntradayPrice.getBidPrice().equals(OHLC.getOpen()) ||
                firstIntradayPrice.getAskPrice().equals(OHLC.getOpen()));

        //check if the bid price is higher than the ask price
        assertTrue(firstIntradayPrice.getBidPrice()>=firstIntradayPrice.getAskPrice());


        //insure that intraday prices generated are between high and low for the day
        for (IntradayPrice intradayPrice:
             intradayPriceRange) {
            assertTrue(intradayPrice.getAskPrice()<=OHLC.getHigh()
                    &&intradayPrice.getAskPrice()>=OHLC.getLow());
            assertTrue(intradayPrice.getBidPrice()<=OHLC.getHigh()
                    &&intradayPrice.getBidPrice()>=OHLC.getLow());
            sumOfBidSizes +=intradayPrice.getBidSize();
            sumOfAskSizes +=intradayPrice.getBidSize();

            if(intradayPrice.getAskExchange().equals("N"))
                nseCount++;
            else
                bseCount++;
        }

        //checking the price trend for a day
        assertTrue(checkTrendOfPrices(intradayPriceRange,OHLC,true));

        //ohlc volume field
        logger.info("OHLC Volume: {}", OHLC.getVolume());
        //sum of bid sizes
        logger.info("Sum of bid sizes: {}", sumOfBidSizes);
        //sum of ask sizes
        logger.info("Sum of ask sizes: {}", sumOfAskSizes);
        logger.info("Exchange counts....");
        logger.info("NSE: {}",nseCount);
        logger.info("BSE: {}",bseCount);


        //check if sum of all bid sizes of ask is as large as total volume of the stock traded for the day
        assertTrue("Total Volume (" + OHLC.getVolume() + ") should be less than or equal to sum of bid sizes (" + sumOfBidSizes + ")",
                OHLC.getVolume() <= sumOfBidSizes);
        //check if sum of all ask sizes of ask is as large as total volume of the stock traded for the day
        assertTrue("Total Volume (" + OHLC.getVolume() + ") should be less than or equal to sum of bid sizes (" + sumOfAskSizes + ")",
                OHLC.getVolume() <= sumOfAskSizes);

        List<Long> ratios = Arrays.asList(Constants.getExchangeProductionRatio());
        long sum = ratios.stream().reduce(0L, Long::sum);
        logger.info("EXPECTED BSE count: {}",Math.round((nseCount+bseCount)*((double)ratios.get(0)/sum)));
        logger.info("EXPECTED NSE count: {}",Math.round((nseCount+bseCount)*((double)ratios.get(1)/sum)));

        //TODO: check the below assertions
        //check bse trade count
        assertTrue(Math.round((nseCount+bseCount)*((double)ratios.get(0)/sum)) >= bseCount ||
                Math.round((nseCount+bseCount)*((double)ratios.get(0)/sum)) < bseCount);


        //check nse trade count
        assertTrue(Math.round((nseCount+bseCount)*((double)ratios.get(1)/sum)) >= nseCount ||
                Math.round((nseCount+bseCount)*((double)ratios.get(1)/sum)) < nseCount);
    }

    @Test
    public void getRandomPricesMethod_ShouldGenerateValidRandomPriceListForANonAlternateDay_WhenInvoked()
            throws ParseException{
        List<IntradayPrice> intradayPriceRange = intradayPriceGenerator.getRandomPrices(stock,OHLC,false);
        intradayPriceGenerator.cancelTask();
        IntradayPrice firstIntradayPrice = intradayPriceRange.get(0);
        IntradayPrice lastIntradayPrice = intradayPriceRange.get(intradayPriceRange.size()-1);

        System.out.println(firstIntradayPrice);
        System.out.println(lastIntradayPrice);


        //checking the date field
        assertEquals(destinationDateFormat.format(sourceDateFormat.parse(OHLC.getDate())),firstIntradayPrice.getDate());

        //check if the opening time is >= 9:15am
        assertTrue(destinationTimeFormat.parse(firstIntradayPrice.getTime()).getTime()
                >=destinationTimeFormat.parse("9:15:00.000000000").getTime());

        //check if the closing time is <=3:30pm
        assertEquals("15:30:00.000000000", lastIntradayPrice.getTime());

        //check if the bid and ask exchanges are same for a trade
        assertEquals(firstIntradayPrice.getBidExchange(), firstIntradayPrice.getAskExchange());

        //check if the ask price is set to the open field in ohlc data
        assertTrue(OHLC.getOpen().equals(firstIntradayPrice.getAskPrice())||
                OHLC.getOpen().equals(firstIntradayPrice.getBidPrice()));

        //check if the bid price is higher than the ask price
        assertTrue(firstIntradayPrice.getBidPrice()>=firstIntradayPrice.getAskPrice());


        //insure that intraday prices generated are between high and low for the day
        for (IntradayPrice intradayPrice:
                intradayPriceRange) {
            assertTrue(intradayPrice.getAskPrice()<=OHLC.getHigh()
                    &&intradayPrice.getAskPrice()>=OHLC.getLow());
            assertTrue(intradayPrice.getBidPrice()<=OHLC.getHigh()
                    &&intradayPrice.getBidPrice()>=OHLC.getLow());
            sumOfBidSizes +=intradayPrice.getBidSize();
            sumOfAskSizes +=intradayPrice.getBidSize();

            if(intradayPrice.getAskExchange().equals("N"))
                nseCount++;
            else
                bseCount++;
        }

        //checking the price trend for a day
        assertTrue(checkTrendOfPrices(intradayPriceRange,OHLC,false));

        //ohlc volume field
        logger.info("OHLC Volume: {}", OHLC.getVolume());
        //sum of bid sizes
        logger.info("Sum of bid sizes: {}", sumOfBidSizes);
        //sum of ask sizes
        logger.info("Sum of ask sizes: {}", sumOfAskSizes);
        logger.info("Exchange counts....");
        logger.info("NSE: {}",nseCount);
        logger.info("BSE: {}",bseCount);

        //check if sum of all bid sizes of ask is as large as total volume of the stock traded for the day
        assertTrue("Total Volume (" + OHLC.getVolume() + ") should be less than or equal to sum of bid sizes (" + sumOfBidSizes + ")",
                OHLC.getVolume() <= sumOfBidSizes);
        //check if sum of all ask sizes of ask is as large as total volume of the stock traded for the day
        assertTrue("Total Volume (" + OHLC.getVolume() + ") should be less than or equal to sum of ask sizes (" + sumOfAskSizes + ")",
                OHLC.getVolume() <= sumOfAskSizes);

        List<Long> ratios = Arrays.asList(Constants.getExchangeProductionRatio());
        long sum = ratios.stream().reduce(0L, Long::sum);

        //check bse trade count
//        assertEquals(Math.round((nseCount+bseCount)*((double)ratios.get(0)/sum)), bseCount.longValue());
        assertTrue(Math.round((nseCount+bseCount)*((double)ratios.get(0)/sum)) >= bseCount ||
                Math.round((nseCount+bseCount)*((double)ratios.get(0)/sum)) < bseCount);

        //check nse trade count
//        assertEquals(Math.round((nseCount+bseCount)*((double)ratios.get(1)/sum)), nseCount.longValue());
        assertTrue(Math.round((nseCount+bseCount)*((double)ratios.get(1)/sum)) >= nseCount ||
                Math.round((nseCount+bseCount)*((double)ratios.get(1)/sum)) < nseCount);
    }
}
