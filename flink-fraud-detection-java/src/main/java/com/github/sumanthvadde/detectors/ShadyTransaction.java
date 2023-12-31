package com.github.sumanthvadde.detectors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.github.sumanthvadde.dto.*;

public class ShadyTransaction extends KeyedProcessFunction<String, CardTransaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 20.00;
	private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    
	private transient ValueState<Boolean> flagState;
	private transient ValueState<Long> timerState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
			"flag",
			Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
			"timer-state",
			Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	@Override
	public void processElement(CardTransaction transaction, KeyedProcessFunction<String, CardTransaction, Alert>.Context context, Collector<Alert> collector) throws Exception {
	
		Boolean lastTransactionWasSmall = flagState.value();
	
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				Alert alert = new Alert();
				alert.setTransaction(transaction);
				alert.setReason("Large amount after small amount");

				collector.collect(alert);
			}
			cleanUp(context);
		}

		
		if (transaction.getAmount() < SMALL_AMOUNT) {
			flagState.update(true);
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer);
		}
	}
	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<Alert> out) {
		timerState.clear();
		flagState.clear();
	}

	private void cleanUp(Context ctx) throws Exception {
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);
		timerState.clear();
		flagState.clear();
	}
}
