package com.orhundalabasmaz.storm.aspects;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class ElapsedTimeAspect {

	private long totalTime = 0;     // ns
	private long totalCount = 0;

//	@Around("execution(* org.apache.storm.grouping.CustomStreamGrouping.chooseTasks(..))")
//	@Around("execution(* com.orhundalabasmaz.storm.loadBalancer.bolts.WorkerBolt.addCountry(..))")
	public Object aroundAdvice(ProceedingJoinPoint joinPoint) throws Throwable {
		Object returnObject = null;
		long begin, end, elapsedTime;
		try {
			begin = System.nanoTime();
			returnObject = joinPoint.proceed();
		} finally {
			end = System.nanoTime();
		}
		elapsedTime = end - begin;

		totalTime += elapsedTime;
		totalCount++;
		double avgElapsedTime = (double) totalTime / totalCount;
		System.out.printf(
				"Curr elapsed time: %7.2f mics -- Avg elapsed time: %7.2f mics\n",
				(double) elapsedTime / 1000, avgElapsedTime / 1000);
		return returnObject;
	}

}