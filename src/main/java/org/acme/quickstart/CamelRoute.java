package org.acme.quickstart;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class CamelRoute extends RouteBuilder {

  @Override
    public void configure() throws Exception {
      errorHandler(defaultErrorHandler().redeliveryDelay(3000).maximumRedeliveries(1));

      ThreadPoolBuilder builder = new ThreadPoolBuilder(getContext());
      ExecutorService pool = builder.poolSize(40).maxPoolSize(80).keepAliveTime(Long.valueOf("5"), TimeUnit.MINUTES)
          .rejectedPolicy(ThreadPoolRejectedPolicy.CallerRuns).build("xxx");
  
      from("timer://trigger?period=200").
              process(exchange -> {
                  exchange.getOut().setBody(Arrays.asList(1, 2, 3, 4, 2, 5, 6, 2, 10, 11));
              }).
              split(body(), new SimpleAggregator())              
              .parallelAggregate()
              .executorService(pool)
              .process(exchange1 -> {
                  System.out.println(Thread.currentThread() + " - STARTING" + exchange1.getIn().getBody());                  
                  if (exchange1.getIn().getBody(Integer.class) == 2) {
                      //System.out.println(Thread.currentThread() + " - Headers: " + exchange1.getIn().getHeaders());
                      System.out.println(Thread.currentThread() + " # Of InFlight Exchanges: " + exchange1.getContext().getInflightRepository().size());
                      if (exchange1.getIn().getHeader("CamelRedelivered", Boolean.class) == Boolean.TRUE) {
                          return;
                      }
                      throw new RuntimeException("Error body 2");
                  }
                  Thread.sleep((long) (Math.random() * 5000));
                  System.out.println(Thread.currentThread() + " - ENDING" + exchange1.getIn().getBody());
              })
              .end()
              .to("log:out");
  }

  class SimpleAggregator implements AggregationStrategy {

    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
      // !! Should be thread safe !!
      System.out.println(Thread.currentThread() + " - aggregate ! total threads: " + java.lang.Thread.activeCount());
      return newExchange;
    }
  }

}
