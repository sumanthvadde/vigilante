package com.github.sumanthvadde.detectors;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import  com.github.sumanthvadde.dto.Alert;
import  com.github.sumanthvadde.dto.CardTransaction;

import lombok.var;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class ExpiredCardDetector extends KeyedProcessFunction<String, CardTransaction, Alert>{
    private static final long serialVersionUID = 3L;

    @Override
    public void processElement(CardTransaction transaction,
            KeyedProcessFunction<String, CardTransaction, Alert>.Context context, Collector<Alert> collector) {

        if (isExpired(transaction)) {
            Alert alert = new Alert();
            alert.setReason("Card is expired");
            alert.setTransaction(transaction);
            collector.collect(alert);
        }
    }
    
    private static Boolean isExpired(CardTransaction transaction)
	{
		final var card = transaction.getCard();
		final var expiryYear = card.getExpYear();
		final var expiryMonth = card.getExpMonth();
		final var expiredDate = (expiryMonth == 12) ? new GregorianCalendar(expiryYear + 1, Calendar.JANUARY, 1).getTime()
				: new GregorianCalendar(expiryYear, expiryMonth, 1).getTime();

		final var date = transaction.getUtc();
		return date.after(expiredDate);
	}
}
