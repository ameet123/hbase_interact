package com.anthem.voyager.service;

import com.anthem.voyager.api.QualityApi;
import com.anthem.voyager.config.AppConfig;
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
    private CompositeDisposable compositeDisposable;
    private PublishSubject<String> safeSource;

    @Autowired
    public EventBus(ConcurrencyService concurrencyService, DBInteraction dbInteraction, AppConfig appConfig,
                    QualityApi qualityApi) {
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
//                                    dbInteraction.checkAndPutKeys(
//                                            strings.stream().map(String::getBytes).collect(Collectors.toList()));
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
//        compositeDisposable.dispose();
    }
}