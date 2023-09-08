package com.github.sumanthvadde.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.Date;

@Data
public class CardTransaction {
    @JsonProperty("amount")
    private int amount;
    @JsonProperty("limit_left")
    private int limitLeft;
    @JsonProperty("currency")
    private String currency;
    @JsonProperty("latitude")
    private double latitude;
    @JsonProperty("longitude")
    private double longitude;
    @JsonProperty("card")
    private Card card;
    @JsonProperty("owner")
    private CardOwner owner;
    @JsonProperty("utc")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Date utc;

    public static CardTransaction fromString(String s) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(s, CardTransaction.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getAccountId() {
        return this.getCard().getCardNumber();
    }
}
