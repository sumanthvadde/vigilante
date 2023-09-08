package com.github.sumanthvadde.detectors;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.github.sumanthvadde.dto.Alert;
import com.github.sumanthvadde.dto.CardTransaction;


public class OverLimitDetector extends KeyedProcessFunction<String, CardTransaction, Alert>{
    
    private static final long serialVersionUID = 2L;

    @Override
	public void processElement(CardTransaction transaction, KeyedProcessFunction<String, CardTransaction, Alert>.Context context, Collector<Alert> collector) {
		if (transaction.getAmount() > transaction.getLimitLeft()) {
			Alert alert = new Alert();
			alert.setReason("Transaction amount is over the limit");
			alert.setTransaction(transaction);
			collector.collect(alert);
		}
	}
}
