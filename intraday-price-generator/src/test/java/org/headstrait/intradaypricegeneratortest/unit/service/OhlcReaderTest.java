package org.headstrait.intradaypricegeneratortest.unit.service;

import org.headstrait.intradaypricegenerator.model.Ohlc;
import org.headstrait.intradaypricegenerator.service.OhlcReader;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OhlcReaderTest {

    private OhlcReader ohlcReader;

    @Before
    public void setUp(){
        String ohlcFolderPath = "src\\test\\resources\\ohlc";
        ohlcReader = new OhlcReader(ohlcFolderPath);
    }

    @Test
    public void getOhlcDataForAStock_ShouldReturnAnEmptyList_IfNoDataIsFoundForTheStock(){
        List<Ohlc> actualData = ohlcReader.getOhlcDataForAStock("RANDOM");
        int actualSize = actualData.size();
        assertEquals(0,actualSize);
    }

    @Test
    public void getOhlcDataForAStock_ShouldReturnAListOfOhlcObjects_IfDataIsFoundForTheStock(){
        List<Ohlc> actualData = ohlcReader.getOhlcDataForAStock("ASFA.NS");
        assertEquals(5,actualData.size());
    }

}