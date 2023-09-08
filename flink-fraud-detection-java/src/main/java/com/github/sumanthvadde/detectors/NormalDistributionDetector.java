package com.github.sumanthvadde.detectors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.sumanthvadde.dto.*;;;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class NormalDistributionDetector extends KeyedProcessFunction<String, CardTransaction, Alert> {

	private static final Logger LOG = LoggerFactory.getLogger(NormalDistributionDetector.class);

	private static final long serialVersionUID = 5L;
	private transient ValueState<Double> averageState;
	private transient ValueState<Double> mVarianceState;
	private transient ValueState<Integer> countState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Double> averageDescriptor = new ValueStateDescriptor<>(
			"average",
			Types.DOUBLE);
		averageState = getRuntimeContext().getState(averageDescriptor);

		ValueStateDescriptor<Double> mVarianceDescriptor = new ValueStateDescriptor<>(
			"variance",
			Types.DOUBLE);
		mVarianceState = getRuntimeContext().getState(mVarianceDescriptor);

		ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
			"count",
			Types.INT);
		countState = getRuntimeContext().getState(countDescriptor);
	}

	@Override
	public void processElement(CardTransaction transaction, KeyedProcessFunction<String, CardTransaction, Alert>.Context context, Collector<Alert> collector) throws Exception {
		Double average = averageState.value();
		Double mVariance = mVarianceState.value();
		Integer count = countState.value();

		if (count == null) {
			count = 1;
		} else {
			count++;
		}

		countState.update(count);

		double previousAverage;
		if (average == null) {
			average = (double) transaction.getAmount();
			previousAverage = average;
		} else {
			previousAverage = average;
			average = average + (transaction.getAmount() - average) / count;
		}

		averageState.update(average);

		double previousStandardDeviation = 0.0;
		if (count >= 2) {
			if (mVariance == null) {
				mVariance = 0.0;
			}
			previousStandardDeviation = Math.sqrt(mVariance / count);
			mVariance = mVariance + (transaction.getAmount() - previousAverage) * (transaction.getAmount() - average);
		}

		mVarianceState.update(mVariance);
		double variance = mVariance == null ? 0.0 : mVariance / count;
		double standardDeviation = Math.sqrt(variance);

		LOG.info("Transaction amount: " + transaction.getAmount());
		LOG.info("Average: " + average + ", Variance: " + variance + ", Standard Deviation: " + standardDeviation +  ", Count: " + count);

		if (count > 10 && Math.abs(transaction.getAmount() - previousAverage) > previousStandardDeviation) {
			Alert alert = new Alert();
			alert.setTransaction(transaction);
			alert.setReason("Transaction is out of average range by more than standard deviation");
			collector.collect(alert);
		}
	}
}
