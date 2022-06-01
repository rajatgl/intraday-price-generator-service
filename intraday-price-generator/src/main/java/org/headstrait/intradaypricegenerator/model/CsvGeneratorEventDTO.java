package org.headstrait.intradaypricegenerator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

//this model is used as a wrapper for the price range to be sent onto the message broker.
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CsvGeneratorEventDTO implements Serializable {
    private String fileName;
    private UUID requestId;
    private List<String> headers;
    private String data;
    private int numberOfRows;
}
