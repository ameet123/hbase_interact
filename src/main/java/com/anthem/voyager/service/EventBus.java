package com.anthem.voyager.service;

import com.anthem.voyager.config.AppProperties;
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

@Service
public class EventBus {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventBus.class);
    private   CompositeDisposable compositeDisposable;
    private PublishSubject<String> safeSource;
    private Disposable subscription;

    @Autowired
    public EventBus(ConcurrencyService concurrencyService, DBInteraction dbInteraction) {
        LOGGER.info("Bucketing messages:{} at threshold:{} sec.",
                AppProperties.PUBLISH_BUCKETING_SIZE, AppProperties.PUBLISH_SEC_THRESHOLD);
        safeSource = PublishSubject.create();
        // register a subscriber
        subscription = safeSource.buffer(AppProperties.PUBLISH_SEC_THRESHOLD, TimeUnit.SECONDS, AppProperties
                .PUBLISH_BUCKETING_SIZE).
                observeOn(Schedulers.computation(), true, AppProperties.SOURCE_BUFFER_BACKPRESSURE).
                subscribe(stringList ->
                        Observable.just(stringList).
                                observeOn(Schedulers.from(concurrencyService.executorService())).
                                subscribe(strings -> {
                                    LOGGER.info("\t>>Size of message:{}", strings.size());
                                    dbInteraction.checkAndPut(strings);
                                }));
        compositeDisposable = new CompositeDisposable();
        compositeDisposable.add(subscription);
    }

    public void send(String o) {
        safeSource.onNext(o);
    }

    public void stop() {
        LOGGER.info(">>Stopping the bus");
        compositeDisposable.dispose();
    }
}