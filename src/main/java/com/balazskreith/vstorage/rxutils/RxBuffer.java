package com.balazskreith.vstorage.rxutils;

interface RxBuffer {

    static long NOT_STARTED = -1L;

    int size();

    void emit();

    long started();

    long emitted();

    boolean isDisposed();

}
