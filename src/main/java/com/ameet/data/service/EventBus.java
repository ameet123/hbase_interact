package com.ameet.data.service;

import com.ameet.data.api.QualityApi;
import com.ameet.data.config.AppConfig;
import io.reactivex.Observable;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * this is where an event bus is designed and instantiated.
 * we establish a publish subscribe bus and on receiving a message, we spawn the task on one of the threads available
 * to us. So the heavy processing is off loaded, allowing the system to consume ever present messages.
 */
@Service
public class EventBus {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventBus.class);
    private CompositeDisposable compositeDisposable;
    private PublishSubject<String> safeSource;

    @Autowired
    public EventBus(ConcurrencyService concurrencyService, AppConfig appConfig, QualityApi qualityApi) {
        LOGGER.info("Bucketing messages:{} at threshold:{} sec.",
                appConfig.getBucketSize(), appConfig.getThresholdSec());
        safeSource = PublishSubject.create();
        // register a subscriber
        Disposable subscription = safeSource.
                buffer(appConfig.getThresholdSec(), TimeUnit.SECONDS, appConfig.getBucketSize()).
                observeOn(Schedulers.computation(), true, appConfig.getBackpressure()).
                subscribe(stringList ->
                        Observable.just(stringList).
                                observeOn(Schedulers.from(concurrencyService.executorService())).
                                subscribe(strings -> {
                                    LOGGER.info("\t>>Size of message:{}", strings.size());
                                    qualityApi.ingest(strings);
                                }));

        compositeDisposable = new CompositeDisposable();
        compositeDisposable.add(subscription);
    }

    public void send(String o) {
        safeSource.onNext(o);
    }

    public void stop() {
        LOGGER.info(">>Stopping the bus");
        compositeDisposable.clear();
    }
}