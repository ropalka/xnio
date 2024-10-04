/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2008 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.xnio;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.xnio._private.Messages.futureMsg;

/**
 * An abstract base class for {@code IoFuture} objects.  Used to easily produce implementations.
 *
 * @param <T> the type of result that this operation produces
 */
public abstract class AbstractIoFuture<T> implements IoFuture<T> {

    @SuppressWarnings("unchecked")
    private final AtomicReference<State<T>> stateRef = new AtomicReference<>((State<T>) ST_INITIAL);

    private static final State<?> ST_INITIAL = new InitialState<>();
    private static final State<?> ST_CANCELLED = new CancelledState<>();

    static abstract class State<T> {
        abstract Status getStatus();

        abstract void notifyDone(AbstractIoFuture<T> future, T result);

        abstract void notifyFailed(AbstractIoFuture<T> future, IOException exception);

        abstract void notifyCancelled(AbstractIoFuture<T> future);

        abstract void cancel();

        abstract boolean cancelRequested();

        State<T> withWaiter(final Thread thread) {
            return new WaiterState<T>(this, thread);
        }

        <A> State<T> withNotifier(final Executor executor, final AbstractIoFuture<T> future, final Notifier<? super T, A> notifier, final A attachment) {
            return new NotifierState<T, A>(this, notifier, attachment);
        }

        State<T> withCancelHandler(final Cancellable cancellable) {
            return new CancellableState<T>(this, cancellable);
        }

        T getResult() {
            throw new IllegalStateException();
        }

        IOException getException() {
            throw new IllegalStateException();
        }
    }

    private static abstract class NestedState<T> extends State<T> {
        private final State<T> next;

        public NestedState(final State next) {
            this.next = next;
        }

        /**
         * Perform any actions that need to be executed when future is done, delegation of done notification to next is
         * taken care of by invoker.
         *
         * @param future the future
         * @param result the result
         */
        protected abstract void doNotifyDone(AbstractIoFuture<T> future, T result);

        @Override
        public void notifyDone(AbstractIoFuture<T> future, T result) {
            doNotifyDone(future, result);
            if (next instanceof NestedState) {
                NestedState<T> current = this;
                do {
                    current = (NestedState<T>) current.next;
                    current.doNotifyDone(future, result);
                } while (current.next instanceof NestedState);
                current.next.notifyDone(future, result);
            } else {
                next.notifyDone(future, result);
            }

        }

        /**
         * Perform any actions that need to be done at this state for handling failure, delegation of failure
         * notification to next is taken care of by invoker
         *
         * @param future    the future
         * @param exception the failure
         */
        protected abstract void doNotifyFailed(AbstractIoFuture<T> future, IOException exception);

        @Override
        public void notifyFailed(AbstractIoFuture<T> future, IOException exception) {
            doNotifyFailed(future, exception);
            if (next instanceof NestedState) {
                NestedState<T> current = this;
                do {
                    current = (NestedState<T>) current.next;
                    current.doNotifyFailed(future, exception);
                } while (current.next instanceof NestedState);
                current.next.notifyFailed(future, exception);
            } else {
                next.notifyFailed(future, exception);
            }
        }

        /**
         * Perform any actions that need to be done at this state for handling cancellation, delegation of cancellation
         * notification to next is taken care of by invoker
         *
         * @param future the future
         */
        protected abstract void doNotifyCancelled(AbstractIoFuture<T> future);



        @Override
        public void notifyCancelled(AbstractIoFuture<T> future) {
            doNotifyCancelled(future);
            if (next instanceof NestedState) {
                NestedState<T> current = this;
                do {
                    current = (NestedState<T>) current.next;
                    current.doNotifyCancelled(future);
                } while (current.next instanceof NestedState);
                current.next.notifyCancelled(future);
            } else {
                next.notifyCancelled(future);
            }
        }

        /**
         * Perform any actions that need to be done at this state for cancellation. Delegation of cancellation to next
         * is taken care of by invoker.
         */
        protected abstract void doCancel();

        /**
         * Just delegate cancel() to first next state in the nested chain that is not a NestedState.
         */
        @Override
        public void cancel() {
            doCancel();
            if (next instanceof NestedState) {
                NestedState<T> current = this;
                do {
                    current = (NestedState<T>) current.next;
                    current.doCancel();
                } while (current.next instanceof NestedState);
                current.next.cancel();
            } else {
                next.cancel();
            }
        }

        /**
         * Return {@code true} to indicate that, at this state, cancel is requested. If returns false, invoker
         * will check for inner next states in the chain until it finds a positive result or the final state in the
         * chain.
         *
         * @return {@code true} to indicate that cancel is requested; {@code false} to delegate response to nested
         *         state.
         */
        protected abstract boolean isCancelRequested();

        @Override
        public boolean cancelRequested() {
            if (isCancelRequested()) {
                return true;
            }
            if (next instanceof NestedState) {
                NestedState<T> current = this;
                do {
                    current = (NestedState<T>) current.next;
                    if (current.isCancelRequested()) {
                        return true;
                    }
                } while (current.next instanceof NestedState);
                return current.next.cancelRequested();
            } else {
                return next.cancelRequested();
            }
        }
    }

    static final class InitialState<T> extends State<T> {

        Status getStatus() {
            return Status.WAITING;
        }

        void notifyDone(final AbstractIoFuture<T> future, final T result) {
        }

        void notifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
        }

        void notifyCancelled(final AbstractIoFuture<T> future) {
        }

        void cancel() {
        }

        boolean cancelRequested() {
            return false;
        }
    }

    static final class CompleteState<T> extends State<T> {
        private final T result;

        CompleteState(final T result) {
            this.result = result;
        }

        Status getStatus() {
            return Status.DONE;
        }

        void notifyDone(final AbstractIoFuture<T> future, final T result) {
        }

        void notifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
        }

        void notifyCancelled(final AbstractIoFuture<T> future) {
        }

        void cancel() {
        }

        State<T> withCancelHandler(final Cancellable cancellable) {
            return this;
        }

        State<T> withWaiter(final Thread thread) {
            return this;
        }

        <A> State<T> withNotifier(final Executor executor, final AbstractIoFuture<T> future, final Notifier<? super T, A> notifier, final A attachment) {
            future.runNotifier(new NotifierRunnable<T, A>(notifier, future, attachment));
            return this;
        }

        T getResult() {
            return result;
        }

        boolean cancelRequested() {
            return false;
        }
    }

    static final class FailedState<T> extends State<T> {
        private final IOException exception;

        FailedState(final IOException exception) {
            this.exception = exception;
        }

        Status getStatus() {
            return Status.FAILED;
        }

        void notifyDone(final AbstractIoFuture<T> future, final T result) {
        }

        void notifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
        }

        void notifyCancelled(final AbstractIoFuture<T> future) {
        }

        void cancel() {
        }

        State<T> withCancelHandler(final Cancellable cancellable) {
            return this;
        }

        State<T> withWaiter(final Thread thread) {
            return this;
        }

        <A> State<T> withNotifier(final Executor executor, final AbstractIoFuture<T> future, final Notifier<? super T, A> notifier, final A attachment) {
            future.runNotifier(new NotifierRunnable<T, A>(notifier, future, attachment));
            return this;
        }

        IOException getException() {
            return exception;
        }

        boolean cancelRequested() {
            return false;
        }
    }

    static final class CancelledState<T> extends State<T> {

        CancelledState() {
        }

        Status getStatus() {
            return Status.CANCELLED;
        }

        void notifyDone(final AbstractIoFuture<T> future, final T result) {
        }

        void notifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
        }

        void notifyCancelled(final AbstractIoFuture<T> future) {
        }

        void cancel() {
        }

        State<T> withCancelHandler(final Cancellable cancellable) {
            try {
                cancellable.cancel();
            } catch (Throwable ignored) {}
            return this;
        }

        <A> State<T> withNotifier(final Executor executor, final AbstractIoFuture<T> future, final Notifier<? super T, A> notifier, final A attachment) {
            future.runNotifier(new NotifierRunnable<T, A>(notifier, future, attachment));
            return this;
        }

        State<T> withWaiter(final Thread thread) {
            return this;
        }

        boolean cancelRequested() {
            return true;
        }
    }

    static final class NotifierState<T, A> extends NestedState<T> {
        final Notifier<? super T, A> notifier;
        final A attachment;

        NotifierState(final State<T> next, final Notifier<? super T, A> notifier, final A attachment) {
            super(next);
            this.notifier = notifier;
            this.attachment = attachment;
        }

        Status getStatus() {
            return Status.WAITING;
        }

        @Override
        protected void doNotifyDone(final AbstractIoFuture<T> future, final T result) {
            doNotify(future);
        }

        @Override
        protected void doNotifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
            doNotify(future);
        }

        @Override
        protected void doNotifyCancelled(final AbstractIoFuture<T> future) {
            doNotify(future);
        }

        @Override
        protected void doCancel() {
        }

        private void doNotify(final AbstractIoFuture<T> future) {
            future.runNotifier(new NotifierRunnable<T, A>(notifier, future, attachment));
        }

        @Override
        protected boolean isCancelRequested() {
            return false;
        }
    }

    static final class WaiterState<T> extends NestedState<T> {
        final Thread waiter;

        WaiterState(final State<T> next, final Thread waiter) {
            super(next);
            this.waiter = waiter;
        }

        Status getStatus() {
            return Status.WAITING;
        }

        @Override
        protected void doNotifyDone(final AbstractIoFuture<T> future, final T result) {
            LockSupport.unpark(waiter);
        }

        @Override
        protected void doNotifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
            LockSupport.unpark(waiter);
        }

        @Override
        protected void doNotifyCancelled(final AbstractIoFuture<T> future) {
            LockSupport.unpark(waiter);
        }

        @Override
        protected void doCancel() {}

        @Override
        protected boolean isCancelRequested() {
            return false;
        }
    }

    static final class CancellableState<T> extends NestedState<T> {
        final Cancellable cancellable;

        CancellableState(final State<T> next, final Cancellable cancellable) {
            super(next);
            this.cancellable = cancellable;
        }

        Status getStatus() {
            return Status.WAITING;
        }

        @Override
        protected void doNotifyDone(final AbstractIoFuture<T> future, final T result) {
        }

        @Override
        protected void doNotifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
        }

        @Override
        protected void doNotifyCancelled(final AbstractIoFuture<T> future) {
        }

        @Override
        protected void doCancel() {
            try {
                cancellable.cancel();
            } catch (Throwable ignored) {}
        }

        @Override
        protected boolean isCancelRequested() {
            return false;
        }
    }

    static final class CancelRequestedState<T> extends NestedState<T> {

        CancelRequestedState(final State<T> next) {
            super(next);
        }

        Status getStatus() {
            return Status.WAITING;
        }

        @Override
        protected void doNotifyDone(final AbstractIoFuture<T> future, final T result) {
        }

        @Override
        protected void doNotifyFailed(final AbstractIoFuture<T> future, final IOException exception) {
        }

        @Override
        protected void doNotifyCancelled(final AbstractIoFuture<T> future) {
        }

        @Override
        protected void doCancel() {
            // terminate
        }

        @Override
        protected boolean isCancelRequested() {
            return true;
        }
    }

    /**
     * Construct a new instance.
     */
    protected AbstractIoFuture() {
    }

    /**
     * {@inheritDoc}
     */
    public Status getStatus() {
        return getState().getStatus();
    }

    private State<T> getState() {
        return stateRef.get();
    }

    private boolean compareAndSetState(State<T> expect, State<T> update) {
        return stateRef.compareAndSet(expect, update);
    }

    /**
     * {@inheritDoc}
     */
    public Status await() {
        final Thread thread = Thread.currentThread();
        State<T> state;
        for (;;) {
            state = getState();
            if (state.getStatus() != Status.WAITING) {
                return state.getStatus();
            }
            Xnio.checkBlockingAllowed();
            State<T> withWaiter = state.withWaiter(thread);
            if (compareAndSetState(state, withWaiter)) {
                boolean intr = Thread.interrupted();
                try {
                    do {
                        LockSupport.park(this);
                        if (Thread.interrupted()) intr = true;
                        state = getState();
                    } while (state.getStatus() == Status.WAITING);
                    return state.getStatus();
                } finally {
                    if (intr) thread.interrupt();
                }
            }
            // retry
        }
    }

    /**
     * {@inheritDoc}
     */
    public Status await(long time, final TimeUnit timeUnit) {
        if (time < 0L) {
            time = 0L;
        }
        long duration = timeUnit.toNanos(time);
        long now = System.nanoTime();
        long tick;
        final Thread thread = Thread.currentThread();
        State<T> state;
        for (;;) {
            state = getState();
            if (state.getStatus() != Status.WAITING || duration == 0L) {
                return state.getStatus();
            }
            Xnio.checkBlockingAllowed();
            State<T> withWaiter = state.withWaiter(thread);
            if (compareAndSetState(state, withWaiter)) {
                boolean intr = Thread.interrupted();
                try {
                    do {
                        LockSupport.parkNanos(this, duration);
                        if (Thread.interrupted()) intr = true;
                        state = getState();
                        duration -= (tick = System.nanoTime()) - now;
                        now = tick;
                    } while (state.getStatus() == Status.WAITING && duration > 0L);
                    return state.getStatus();
                } finally {
                    if (intr) thread.interrupt();
                }
            }
            // retry
        }
    }

    /**
     * {@inheritDoc}
     */
    public Status awaitInterruptibly() throws InterruptedException {
        final Thread thread = Thread.currentThread();
        State<T> state;
        for (;;) {
            state = getState();
            if (state.getStatus() != Status.WAITING) {
                return state.getStatus();
            }
            Xnio.checkBlockingAllowed();
            if (Thread.interrupted()) throw new InterruptedException();
            State<T> withWaiter = state.withWaiter(thread);
            if (compareAndSetState(state, withWaiter)) {
                do {
                    LockSupport.park(this);
                    if (Thread.interrupted()) throw new InterruptedException();
                    state = getState();
                } while (state.getStatus() == Status.WAITING);
                return state.getStatus();
            }
            // retry
        }
    }

    /**
     * {@inheritDoc}
     */
    public Status awaitInterruptibly(long time, final TimeUnit timeUnit) throws InterruptedException {
        if (time < 0L) {
            time = 0L;
        }
        long duration = timeUnit.toNanos(time);
        long now = System.nanoTime();
        long tick;
        final Thread thread = Thread.currentThread();
        State<T> state;
        for (;;) {
            state = getState();
            if (state.getStatus() != Status.WAITING || duration == 0L) {
                return state.getStatus();
            }
            Xnio.checkBlockingAllowed();
            if (Thread.interrupted()) throw new InterruptedException();
            State<T> withWaiter = state.withWaiter(thread);
            if (compareAndSetState(state, withWaiter)) {
                do {
                    LockSupport.parkNanos(this, duration);
                    if (Thread.interrupted()) throw new InterruptedException();
                    state = getState();
                    duration -= (tick = System.nanoTime()) - now;
                    now = tick;
                } while (state.getStatus() == Status.WAITING && duration > 0L);
                return state.getStatus();
            }
            // retry
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked"})
    public T get() throws IOException, CancellationException {
        switch (await()) {
            case DONE: return getState().getResult();
            case FAILED: throw getState().getException();
            case CANCELLED: throw futureMsg.opCancelled();
            default: throw new IllegalStateException();
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked"})
    public T getInterruptibly() throws IOException, InterruptedException, CancellationException {
        switch (awaitInterruptibly()) {
            case DONE: return getState().getResult();
            case FAILED: throw getState().getException();
            case CANCELLED: throw futureMsg.opCancelled();
            default: throw new IllegalStateException();
        }
    }

    /**
     * {@inheritDoc}
     */
    public IOException getException() throws IllegalStateException {
        return getState().getException();
    }

    /**
     * {@inheritDoc}
     */
    public <A> IoFuture<T> addNotifier(final Notifier<? super T, A> notifier, final A attachment) {
        State<T> oldState, newState;
        do {
            oldState = getState();
            newState = oldState.withNotifier(getNotifierExecutor(), this, notifier, attachment);
        } while (! compareAndSetState(oldState, newState));
        return this;
    }

    /**
     * Set the exception for this operation.  Any threads blocking on this instance will be unblocked.
     *
     * @param exception the exception to set
     * @return {@code false} if the operation was already completed, {@code true} otherwise
     */
    protected boolean setException(IOException exception) {
        State<T> oldState;
        oldState = getState();
        if (oldState.getStatus() != Status.WAITING) {
            return false;
        } else {
            State<T> newState = new FailedState<T>(exception);
            while (! compareAndSetState(oldState, newState)) {
                oldState = getState();
                if (oldState.getStatus() != Status.WAITING) {
                    return false;
                }
            }
        }
        oldState.notifyFailed(this, exception);
        return true;
    }

    /**
     * Set the result for this operation.  Any threads blocking on this instance will be unblocked.
     *
     * @param result the result to set
     * @return {@code false} if the operation was already completed, {@code true} otherwise
     */
    protected boolean setResult(T result) {
        State<T> oldState;
        oldState = getState();
        if (oldState.getStatus() != Status.WAITING) {
            return false;
        } else {
            State<T> newState = new CompleteState<>(result);
            while (! compareAndSetState(oldState, newState)) {
                oldState = getState();
                if (oldState.getStatus() != Status.WAITING) {
                    return false;
                }
            }
        }
        oldState.notifyDone(this, result);
        return true;
    }

    /**
     * Acknowledge the cancellation of this operation.
     *
     * @return {@code false} if the operation was already completed, {@code true} otherwise
     */
    protected boolean setCancelled() {
        State<T> oldState;
        oldState = getState();
        if (oldState.getStatus() != Status.WAITING) {
            return false;
        } else {
            @SuppressWarnings("unchecked")
            State<T> newState = (State<T>) ST_CANCELLED;
            while (! compareAndSetState(oldState, newState)) {
                oldState = getState();
                if (oldState.getStatus() != Status.WAITING) {
                    return false;
                }
            }
        }
        oldState.notifyCancelled(this);
        return true;
    }

    /**
     * Cancel an operation.  The actual cancel may be synchronous or asynchronous.  Implementers will use this method
     * to initiate the cancel; use the {@link #setCancelled()} method to indicate that the cancel was successful.  The
     * default implementation calls any registered cancel handlers.
     *
     * @return this {@code IoFuture} instance
     */
    public IoFuture<T> cancel() {
        State<T> state;
        do {
            state = getState();
            if (state.getStatus() != Status.WAITING || state.cancelRequested()) return this;
        } while (! compareAndSetState(state, new CancelRequestedState<T>(state)));
        state.cancel();
        return this;
    }

    /**
     * Add a cancellation handler.  The argument will be cancelled whenever this {@code IoFuture} is cancelled.  If
     * the {@code IoFuture} is already cancelled when this method is called, the handler will be called directly.
     *
     * @param cancellable the cancel handler
     */
    protected void addCancelHandler(final Cancellable cancellable) {
        State<T> oldState, newState;
        do {
            oldState = getState();
            if (oldState.getStatus() != Status.WAITING || oldState.cancelRequested()) {
                try {
                    cancellable.cancel();
                } catch (Throwable ignored) {
                }
                return;
            }
            newState = oldState.withCancelHandler(cancellable);
            if (oldState == newState) return;
        } while (! compareAndSetState(oldState, newState));
    }

    /**
     * Run a notifier.  Implementors will run the notifier, preferably in another thread.  The default implementation
     * runs the notifier using the {@code Executor} retrieved via {@link #getNotifierExecutor()}.
     *
     * @param runnable the runnable task
     */
    protected void runNotifier(final Runnable runnable) {
        getNotifierExecutor().execute(runnable);
    }

    /**
     * Get the executor used to run asynchronous notifiers.  By default, this implementation simply returns the direct
     * executor.
     *
     * @return the executor to use
     */
    protected Executor getNotifierExecutor() {
        return IoUtils.directExecutor();
    }

    static class NotifierRunnable<T, A> implements Runnable {

        private final Notifier<? super T, A> notifier;
        private final IoFuture<T> future;
        private final A attachment;

        NotifierRunnable(final Notifier<? super T, A> notifier, final IoFuture<T> future, final A attachment) {
            this.notifier = notifier;
            this.future = future;
            this.attachment = attachment;
        }

        public void run() {
            try {
                notifier.notify(future, attachment);
            } catch (Throwable t) {
                futureMsg.notifierFailed(t, notifier, attachment);
            }
        }
    }
}
