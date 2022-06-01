package org.headstrait.intradaypricegenerator.service;

import org.headstrait.intradaypricegenerator.model.Ohlc;
import org.headstrait.intradaypricegenerator.service.interfaces.IOhlcReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class OhlcReader implements IOhlcReader {

    private final String ohlcFolderPath;
    private final File[] ohlcFiles;


    //logger
    private final Logger log = LoggerFactory.getLogger(OhlcReader.class);



    public OhlcReader(String ohlcFolderPath) {
        this.ohlcFolderPath = ohlcFolderPath;
        ohlcFiles = new File(ohlcFolderPath).listFiles();
    }

    /**
     *
     * @param stock stock symbol of stock whose ohlc data is to be fetched
     * @return list of ohlc objects for various dates
     */
    public List<Ohlc> getOhlcDataForAStock(String stock) {
        //ohlc object read form the csv lines
        Ohlc ohlc;

        //initialize the ohlc list
        List<Ohlc> ohlcs = new ArrayList<>();


        //ensure that the list is non-null and non empty
        if (ohlcFiles != null && ohlcFiles.length != 0) {

            //filter through the files and get the file related to the provided stock
            Optional<File> ohlcOfAStockOptional = Arrays.stream(ohlcFiles)
                    .filter(ohlcFile -> ohlcFile.getName().toLowerCase()
                            .contains(stock.toLowerCase()))
                    .findFirst();

            //if found
            if (ohlcOfAStockOptional.isPresent()) {
                String ohlcFileName = ohlcOfAStockOptional.get().getName();
                final Path filePath = Paths.get(ohlcFolderPath + "/" + ohlcFileName);
                log.info("FILE PATH: {}", filePath);
                try(Stream<String>  dataStream = Files.lines(filePath)) {
                    List<String> ohlcData = dataStream
                            .collect(Collectors.toList());
                    ohlcData.remove(0);
                    for (String line :
                            ohlcData) {
                        String[] ohlcStrings = line.split(",");
                        //build the ohlc object
                        ohlc = new Ohlc(ohlcStrings[0],
                                Double.parseDouble(ohlcStrings[1]),
                                Double.parseDouble(ohlcStrings[2]),
                                Double.parseDouble(ohlcStrings[3]),
                                Double.parseDouble(ohlcStrings[4]),
                                Double.parseDouble(ohlcStrings[5]),
                                Long.parseLong(ohlcStrings[6]));
                        //and add it to the list
                        ohlcs.add(ohlc);
                    }
                }
                //catch any file read errors
                catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
            //if not-found
            else {
                log.warn("No data available for the requested stock");
            }
        }
        //return the ohlc list
        return ohlcs;
    }
}

