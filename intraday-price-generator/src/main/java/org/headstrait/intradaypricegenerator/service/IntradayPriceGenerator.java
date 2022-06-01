package org.headstrait.intradaypricegenerator.service;

import org.headstrait.intradaypricegenerator.constants.Constants;
import org.headstrait.intradaypricegenerator.model.IntradayPrice;
import org.headstrait.intradaypricegenerator.model.Ohlc;
import org.headstrait.intradaypricegenerator.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class IntradayPriceGenerator {

    private boolean isGenerating;

    private final Logger log = LoggerFactory.getLogger(IntradayPriceGenerator.class);

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");

    Random random = new Random();

    /**
     * Map to keep track of the price of a stock intermittently
     * The key for each map is the stock symbol
     */
    //keeps track of the stock price wave: if the stock price already hit the high, or hit the low
    private final Map<String, Pair<Boolean, Boolean>> stockPriceWave = new HashMap<>();

    //setter
    public void setGenerating(boolean generating) {
        isGenerating = generating;
    }


    /**
     * Sets in case the stock has already hit a 'high' and should now start decreasing in price
     *
     * @param stock the stock symbol
     */
    private void setHighDone(String stock) {
        stockPriceWave.put(stock, new Pair<>(true, stockPriceWave.get(stock).getValue()));
    }

    /**
     * Sets in case the stock has already hit a 'low' and should now start increasing in price
     *
     * @param stock the stock symbol
     */
    private void setLowDone(String stock) {
        stockPriceWave.put(stock, new Pair<>(stockPriceWave.get(stock).getKey(), true));
    }

    /**
     * checks if high was reached
     * @param stock symbol
     * @return true if high for the day was touched, false if not.
     */
    //getters for the above two properties: fetched from the map 'stockPriceWave'
    private boolean isHighDone(String stock) {
        stockPriceWave.putIfAbsent(stock, new Pair<>(false, false));
        return stockPriceWave.get(stock).getKey();

    }

    /**
     * checks if the low has been reached by the given stock.
     * @param stock symbol
     * @return true if low for the day was touched else false
     */
    private boolean isLowDone(String stock) {
        stockPriceWave.putIfAbsent(stock, new Pair<>(false, false));
        return stockPriceWave.get(stock).getValue();
    }


    public void cancelTask() {

        setGenerating(false);

    }

    /**
     *
     * @param ohlc for the day
     * @param numberOfSegmentsInADay number of splits
     * @param isAlternateDay up or down trend
     * @return a list of ohlcs to be followed by each segment of the day.
     */
    private List<Ohlc> splitOhlc(Ohlc ohlc, int numberOfSegmentsInADay, boolean isAlternateDay){

        //random integer as the index of first high/low
        int randomFirstIndex = ThreadLocalRandom.current().nextInt(0, numberOfSegmentsInADay/4);

        //random integer as the index of second high/low
        int randomSecondIndex = ThreadLocalRandom.current().nextInt(numberOfSegmentsInADay/2, (3*numberOfSegmentsInADay)/4);

        //select random segments for high and low
        int high;
        int low;
        if(isAlternateDay) {
            high = randomFirstIndex;
            low = randomSecondIndex;
        }
        else {
            high = randomSecondIndex;
            low = randomFirstIndex;
        }

        //to determine high and low for segment ohlcs
        double ratio = Math.abs(ohlc.getOpen() - ohlc.getClose())/ohlc.getOpen();

        //for each segment of the day, create a random ohlc.
        List<Ohlc> ohlcs = new ArrayList<>();

        for(int i=0;i<=numberOfSegmentsInADay;i++) {
            Ohlc segmentOhlc = new Ohlc();
            segmentOhlc.setVolume((long) Math.ceil((double)ohlc.getVolume()/numberOfSegmentsInADay) + ThreadLocalRandom.current().nextLong(5000, 10000));
            if(i == numberOfSegmentsInADay) {
                segmentOhlc.setVolume((long) Math.ceil((double)ohlc.getVolume()/numberOfSegmentsInADay) + ThreadLocalRandom.current().nextLong(5000, 10000));
            }
            if(i == 0) {
                segmentOhlc.setOpen(ohlc.getOpen());
            }
            else {
                segmentOhlc.setOpen(ohlcs.get(ohlcs.size()-1).getClose());
            }

            //set high and low (with validation)
            segmentOhlc.setHigh(segmentOhlc.getOpen() + (segmentOhlc.getOpen() * ThreadLocalRandom.current().nextDouble(ratio * 0.05, ratio * 0.5)));
            segmentOhlc.setLow(segmentOhlc.getOpen() - (segmentOhlc.getOpen() * ThreadLocalRandom.current().nextDouble(ratio * 0.05, ratio * 0.5)));
            if(segmentOhlc.getHigh() > ohlc.getHigh()) {
                if(!isAlternateDay && i<=low)
                    segmentOhlc.setHigh(ThreadLocalRandom.current().nextDouble(segmentOhlc.getOpen(), ohlc.getHigh()));
                else
                    segmentOhlc.setHigh(ohlc.getHigh());
            }
            if(segmentOhlc.getLow() < ohlc.getLow()) {
                if(isAlternateDay && i<=high)
                    segmentOhlc.setLow(ThreadLocalRandom.current().nextDouble(ohlc.getLow()+0.1, segmentOhlc.getOpen()));
                else
                    segmentOhlc.setLow(ohlc.getLow());
            }
            if(i == high) {
                segmentOhlc.setHigh(ohlc.getHigh());
            }
            if(i == low) {
                segmentOhlc.setLow(ohlc.getLow());
            }

            //set close (with validation)
            segmentOhlc.setClose(ThreadLocalRandom.current().nextDouble(segmentOhlc.getLow()+0.1, segmentOhlc.getHigh()));
            if(i == numberOfSegmentsInADay) {
                segmentOhlc.setClose(ohlc.getClose());
            }

            ohlcs.add(segmentOhlc);
        }

        //return the collected segment ohlcs.
        return ohlcs;
    }


    /**
     * this is the core method of this class.
     *
     * @param stock whos random prices is to be generated
     * @param ohlc object of the day
     * @param isAlternateDay to change the trend of the price wave
     * @return the list of random prices
     */
    public List<IntradayPrice> getRandomPrices(final String stock, final Ohlc ohlc, final boolean isAlternateDay) throws ParseException {

        SimpleDateFormat sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        SimpleDateFormat destinationFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSSSSSSSS");

        log.info("PREPARING INTRADAY PRICES FOR THE STOCK: {} FOR THE DAY: {}", stock, ohlc.getDate());
        List<IntradayPrice> consolidatedIntradayPrices = new ArrayList<>();

        String date = ohlc.getDate() + " 9:15";
        String dateEnd = ohlc.getDate() + " 15:30";

        //random number between 7 and 23
        final int NUMBER_OF_SEGMENTS = ThreadLocalRandom.current().nextInt(1, 3) * 8 - 1;

        Long timestampStart = sourceFormat.parse(date).getTime();
        Long timestampEnd = sourceFormat.parse(dateEnd).getTime();

        //to store the open and close timestamp pair for each segment of the day.
        List<Pair<Long, Long>> timestampPairs = new ArrayList<>();
        for(int i=0;i<=NUMBER_OF_SEGMENTS;i++) {
            long start;
            long end;
            if(i == 0) {
                start = timestampStart;
            }
            else {
                start = timestampPairs.get(timestampPairs.size()-1).getValue();
            }
            end = start + (timestampEnd-timestampStart)/(NUMBER_OF_SEGMENTS+1);
            if(i == NUMBER_OF_SEGMENTS) {
                end = timestampEnd;
            }
            timestampPairs.add(new Pair<>(start, end));
        }

        //split the ohlc into multiple segments
        List<Ohlc> ohlcs = splitOhlc(ohlc, NUMBER_OF_SEGMENTS, isAlternateDay);
        assert ohlcs.size() == timestampPairs.size();

        //generate the price range for each segment
        for(int i=0;i<timestampPairs.size();i++) {

            setGenerating(true);
            List<IntradayPrice> intradayPrices = new ArrayList<>();

            long timestamp = timestampPairs.get(i).getKey();

            while (isGenerating) {

                String datetime = destinationFormat.format(new Date(timestamp));

                //if its 3:30pm, send the closing price and mark the remaining volume as traded for the day.
                if (timestamp >= timestampPairs.get(i).getValue()) {
                    IntradayPrice intradayActivity = IntradayPrice.builder()
                            .stockSymbol(stock)
                            .date(datetime.split(" ")[0])
                            .time(destinationFormat.format(new Date(timestampPairs.get(i).getValue())).split(" ")[1])
                            .build();
                    intradayPrices.add(intradayActivity);
                    log.info("BREAKING RANGE GENERATOR.....");
                    break;
                }
                intradayPrices.add(IntradayPrice.builder()
                        .stockSymbol(stock)
                        .date(datetime.split(" ")[0])
                        .time(datetime.split(" ")[1])
                        .build());

                timestamp += ThreadLocalRandom.current().nextLong(300,1000);
            }

            //to adhere to the required exchange ratio and set the bid and ask exchange fields
            exchangeNormalizer(intradayPrices);

            //to ensure that the sum of all bid/ask sizes is as large as the ohlc volume.
            volumeNormalizer(intradayPrices, ohlcs.get(i));

            //to set the bid ans ask prices as per the provided requirement.
            priceNormalizer(intradayPrices, stock, ohlcs.get(i), random.nextBoolean());

            consolidatedIntradayPrices.addAll(intradayPrices);
        }

        //to ensure alternate day trend
        waveNormalizer(consolidatedIntradayPrices, isAlternateDay, ohlc);
        return consolidatedIntradayPrices;
    }

    /**
     *
     * @param intradayPrices consolidated intraday prices to be manipulated to ensure alternate day trend.
     * @param isAlternate check if it is an alternate day
     * @param ohlc details for the day
     */
    private void waveNormalizer(List<IntradayPrice> intradayPrices, boolean isAlternate, Ohlc ohlc){
        log.info("ENTERING WAVE NORMALIZER....");
        boolean isHighDone = false;
        boolean isLowDone = false;
        int index = 0;

        double minDifference = Double.MAX_VALUE;

        if(isAlternate){
            for(int i = 0; i<intradayPrices.size();i++) {
                IntradayPrice intradayPrice = intradayPrices.get(i);
                //set is high done to true if high is hit at some point.
                if(intradayPrice.getAskPrice().equals(ohlc.getHigh()) || intradayPrice.getBidPrice().equals(ohlc.getHigh())){
                    isHighDone = true;
                }
                //if low is hit before high is done then increase that value by some fraction.
                if((intradayPrice.getAskPrice().equals(ohlc.getLow()) || intradayPrice.getBidPrice().equals(ohlc.getLow())) && !isHighDone){
                        if(intradayPrice.getAskPrice().equals(ohlc.getLow()))
                            intradayPrice
                                    .setAskPrice(ohlc.getLow()+ThreadLocalRandom.current().nextDouble(0.1,1));
                        if(intradayPrice.getBidPrice().equals(ohlc.getLow()))
                            intradayPrice
                                    .setBidPrice(ohlc.getLow()+ThreadLocalRandom.current().nextDouble(0.1,1));
                }
                //if high is reached ensure the ohlc low is reached at some point after this.
                if(isHighDone){
                    double difference = intradayPrice.getBidPrice()-ohlc.getLow();
                    if(difference<minDifference) {
                        index = i;
                        minDifference = difference;
                    }
                }
            }
            if(minDifference != 0){
                intradayPrices.get(index).setBidPrice(ohlc.getLow());
            }
        }
        else {
            for(int i = 0; i<intradayPrices.size();i++) {
                IntradayPrice intradayPrice = intradayPrices.get(i);
                //set is low done to true if low is hit at some point.
                if(intradayPrice.getAskPrice().equals(ohlc.getLow()) || intradayPrice.getBidPrice().equals(ohlc.getLow())){
                    isLowDone = true;
                }

                //if high is hit before low then decrease the value by some fraction.
                if((intradayPrice.getAskPrice().equals(ohlc.getHigh()) || intradayPrice.getBidPrice().equals(ohlc.getHigh())) && !isLowDone){
                        if(intradayPrice.getAskPrice().equals(ohlc.getHigh()))
                            intradayPrice
                                    .setAskPrice(ohlc.getHigh()-ThreadLocalRandom.current().nextDouble(0.1,1));
                        if(intradayPrice.getBidPrice().equals(ohlc.getHigh()))
                            intradayPrice
                                    .setBidPrice(ohlc.getHigh()-ThreadLocalRandom.current().nextDouble(0.1,1));
                }
                //if low is done, ensure that ohlc high is reached at some point after this.
                if(isLowDone){
                    double difference = ohlc.getHigh() - intradayPrice.getAskPrice();
                    if(difference<minDifference) {
                        index = i;
                        minDifference = difference;
                    }
                }
            }
            if(minDifference != 0){
                intradayPrices.get(index).setAskPrice(ohlc.getHigh());
            }
        }

        //set the bid and ask prices of first price in the list to ohlc open.
        intradayPrices.get(0).setBidPrice(ohlc.getOpen());
        intradayPrices.get(0).setAskPrice(ohlc.getOpen());

        //set the bid and ask prices of the last price in the list to ohlc close.
        intradayPrices.get(intradayPrices.size()-1).setBidPrice(ohlc.getClose());
        intradayPrices.get(intradayPrices.size()-1).setAskPrice(ohlc.getClose());
    }

    /**
     * this function sets the generated ask and bid exchanges into the intraday price range list.
     * @param intradayPrices with ask and bid exchange fields set
     */
    private void exchangeNormalizer(List<IntradayPrice> intradayPrices) {
        log.info("ENTERING EXCHANGE NORMALIZER....");

        //get the generated exchanges to be set into intraday price object
        List<String> exchanges = generateExchangesWithRatio(Arrays.asList(Constants.getExchangeProductionRatio()), Arrays.asList(Constants.getExchanges()), intradayPrices.size());
        int index = 0;
        //set the generated exchanges to the intraday price bid and ask exchange fields.
        for(IntradayPrice intradayPrice: intradayPrices) {
            intradayPrice.setAskExchange(exchanges.get(index).substring(0,1).toUpperCase());
            intradayPrice.setBidExchange(exchanges.get(index).substring(0,1).toUpperCase());
            index++;
        }
    }

    /**
     *
     * @param price to be formatted into required format
     * @return a formatted price
     */
    private double getFormattedPrice(double price){
        return Double.parseDouble(DECIMAL_FORMAT.format(price));
    }

    /**
     *
     * @param ratios of exchanges to be generated for the given size of price range
     * @param exchanges list of stock exchanges to be generated
     * @param size of the intraday price range list
     * @return the shuffed list of exchanges with the provide ratio of exchanges.
     */
    private List<String> generateExchangesWithRatio(List<Long> ratios, List<String> exchanges, long size) {

        //sum of all numbers in the given ratio
        long sum = ratios.stream().reduce(0L, Long::sum);

        //list to store final counts of exchanges to be generated
        List<Long> finalCounts = new ArrayList<>();

        //update the final count list
        ratios.forEach(ratio -> finalCounts.add(Math.round(size*((double)ratio/sum))));

        //list to store exchanges with their counts matching the final counts list.
        List<String> exchangesWithCount = new ArrayList<>();
        int index = 0;

        //update the exchanges with count list.
        for(Long count: finalCounts) {

            //create 'count' copies of exchange strings to be stored in the exchanges with count list.
            exchangesWithCount.addAll(Collections.nCopies(count.intValue(), exchanges.get(index)));
            index++;
        }

        //shuffle the above collection and return
        Collections.shuffle(exchangesWithCount);
        return exchangesWithCount;
    }

    /**
     *
     * @param intradayPrices price range to be manipulated
     * @param ohlc ohlc prices for the day
     * @param isAlternateDay to decide the price trend
     */
    private void priceNormalizer(List<IntradayPrice> intradayPrices,
                                 String stock, Ohlc ohlc,
                                 boolean isAlternateDay){
        log.info("ENTERING PRICE NORMALIZER....");

        //generate random bid and ask prices.
        List<Pair<Double, Double>> randomPrices = randomPriceGenerator(stock,
                isAlternateDay,
                ohlc,
                intradayPrices);
        int index = 0;
        //loop through each price objects and set the generated bid and ask prices.
        for (IntradayPrice intradayPrice :
                intradayPrices) {
            try {
                intradayPrice.setAskPrice(randomPrices.get(index).getValue());
                intradayPrice.setBidPrice(randomPrices.get(index).getKey());
                index++;
            }
            catch(Exception e) {
                log.error("ERROR reads: {}", e.getMessage());
            }
        }
        log.info("EXITING PRICE NORMALIZER...");
    }


    /**
     *
     * @return returns 1 or -1 with 0.25 percent chances of negation
     */
    private double randomNegate() {
        return (Math.random() > 0.2 ? 1 : -1);
    }

    /**
     * generates list of random prices for a stock
     *
     * @param stock symbol of stock
     * @param isAlternateDay checks if the day is alternate
     * @param ohlc data for the stock for a day
     * @param intradayPrices objects to be set with the generated prices.
     * @return a list of pairs of bid and stock prices to be set into the price object.
     */
    private List<Pair<Double, Double>> randomPriceGenerator(String stock,
                                                            boolean isAlternateDay,
                                                            Ohlc ohlc,
                                                            List<IntradayPrice> intradayPrices) {

        List<Pair<Double, Double>> randomPrices = new ArrayList<>();

        //begin from the opening price of the day.
        double price = ohlc.getOpen();
        boolean isBid = true;
        int priceRangeSize = intradayPrices.size();
        boolean isClosePassed = false;
        for (int i = 0; i < 2 * intradayPrices.size(); i++) {
            //if this is the last element of the price range, set the price to close.
            if (i == (2 * intradayPrices.size() - 1)) {
                price = ohlc.getClose();
            }
            else if (i == 0){
                price = ohlc.getOpen();
            }
            //note: if isAlternate day is true, the wave hits high first then low and then lingers around till it reaches to close.
            //if it is false, the wave hits low first then high and then lingers around till it reaches close.
            else if (isHighDone(stock) && isLowDone(stock)) {
                if (!isClosePassed) {
                    if (ohlc.getClose() < price) {
                        if (isAlternateDay)
                            isClosePassed = true;
                        price -= randomNegate() * random.nextDouble() % (price / (priceRangeSize * (Math.abs(random.nextDouble() % 20) + 10)));
                    } else if (ohlc.getClose() >= price) {
                        if (!isAlternateDay)
                            isClosePassed = true;
                        price += randomNegate() * random.nextDouble() % (price / (priceRangeSize * (Math.abs(random.nextDouble() % 20) + 10)));
                    }
                } else if (ohlc.getClose() < price) {
                    //the modulo term down here is intentionally increased to get some visible randomness in the price wave.
                    price -= Math.abs(random.nextDouble()) % ((price * 10) / (priceRangeSize));
                } else if (ohlc.getClose() >= price) {
                    price += Math.abs(random.nextDouble()) % ((price * 10) / (priceRangeSize));
                }
            } else if (isHighDone(stock)) {
                //if high is reached, start reducing the price.
                price -= randomNegate() * random.nextDouble() % (price / (priceRangeSize * (Math.abs(random.nextDouble() % 20) + 10)));
            } else if (isLowDone(stock)) {
                //if low is reached, start increasing the price.
                price += randomNegate() * random.nextDouble() % (price / (priceRangeSize * (Math.abs(random.nextDouble() % 20) + 10)));
            } else if (isAlternateDay) {
                //if isAlternateDay is true hit high first.
                price += randomNegate() * random.nextDouble() % (price / (priceRangeSize * (Math.abs(random.nextDouble() % 20) + 10)));
            } else {
                //else hit low first.
                price -= randomNegate() * random.nextDouble() % (price / (priceRangeSize * (Math.abs(random.nextDouble() % 20) + 10)));
            }

            if (price > ohlc.getHigh()) {
                //set high done if the price has crossed ohlc high.
                //also set the price to high instead.
                price = ohlc.getHigh();
                setHighDone(stock);
            }
            if (price < ohlc.getLow()) {
                //set low is done if price is below ohlc low.
                //also set the price to low instead.
                price = ohlc.getLow();
                setLowDone(stock);
            }

            if (isBid) {
                //set bid price
                randomPrices.add(i / 2, new Pair<>(getFormattedPrice(price), null));
            } else {
                //set ask price
                if (randomPrices.get(i / 2).getKey() < getFormattedPrice(price)) {
                    randomPrices.get(i / 2).setValue(getFormattedPrice(randomPrices.get(i / 2).getKey()));
                    randomPrices.get(i / 2).setKey(getFormattedPrice(price));
                } else {
                    randomPrices.get(i / 2).setValue(getFormattedPrice(price));
                }
            }
            isBid = !isBid;
        }
        //reset isHighDone and isLowDone for alternate day logic to work.
        stockPriceWave.put(stock, new Pair<>(false, false));

        return randomPrices;
    }


    /**
     * this method sets the bid and ask sizes for the given list
     * @param intradayPrices list of intraday price range
     * @param ohlc details
     */
    private void volumeNormalizer(List<IntradayPrice> intradayPrices, Ohlc ohlc){
        log.info("ENTERING VOLUME NORMALIZER....");
        //get the generated random sizes whoes sum is as large as ohlc volume and set them to the price objects in the list.
        List<Long> sizes = generateRandomSizes(ohlc.getVolume(), intradayPrices.size());
        Collections.shuffle(sizes);
        AtomicLong index = new AtomicLong();
        intradayPrices.parallelStream().forEach(intradayPrice -> {
            intradayPrice.setBidSize(sizes.get(index.intValue()));
            intradayPrice.setAskSize(sizes.get(index.intValue()));
            index.addAndGet(1);
        });
    }

    /**
     * this function ensures that sum of all random sizes is atleast as large as totalSum.
     * @param totalSum of all the random sizes generated.
     * @param totalItems number of random sizes to be generated.
     * @return the list of random sizes
     */
    private List<Long> generateRandomSizes(long totalSum, long totalItems) {

        List<Long> vals = new ArrayList<>();
        totalSum -= totalItems;

        for(long i=0;i<totalItems;i++) {
            vals.add(0L);
        }

        for (long i = 0; i < totalItems-1; ++i) {
            vals.set((int)i, (long)random.nextInt((int)totalSum+1));
        }
        vals.set((int)totalItems-1, totalSum+1);

        Collections.sort(vals);
        for (long i = totalItems-1; i > 0; --i) {
            vals.set((int)i, vals.get((int)i) - vals.get((int)i-1));
        }
        for (int i = 0; i < totalItems; ++i) { vals.set(i, vals.get(i) + 1); }

        //reverse subtraction
        totalSum += totalItems;
        long currentSum = vals.stream().reduce(0L, (subtotal, val) -> subtotal + val);
        int randomIndex = Math.abs(random.nextInt())%(int)totalItems;
        if(currentSum < totalSum) {
            vals.set(randomIndex, vals.get(randomIndex) + (totalSum - currentSum) + (random.nextLong()%20));
        }
        else {
            vals.set(randomIndex, vals.get(randomIndex) - (currentSum - totalSum) + (random.nextLong()%20));
        }
        return vals;
    }
}
