namespace NEventStore.Client
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using NEventStore.Logging;
    using NEventStore.Persistence;

    /// <summary>
    /// Represents a client that poll the storage for latest commits.
    /// </summary>
    public sealed class PollingClient : ClientBase
    {
        private readonly int _interval;

        public PollingClient(IPersistStreams persistStreams, int interval = 5000) : base(persistStreams)
        {
            if (persistStreams == null)
            {
                throw new ArgumentNullException("persistStreams");
            }
            if (interval <= 0)
            {
                throw new ArgumentException(Messages.MustBeGreaterThanZero.FormatWith("interval"));
            }
            _interval = interval;
        }

        /// <summary>
        /// Observe commits from the sepecified checkpoint token. If the token is null,
        /// all commits from the beginning will be observed.
        /// </summary>
        /// <param name="checkpointToken">The checkpoint token.</param>
        /// <returns>
        /// An <see cref="IObserveCommits" /> instance.
        /// </returns>
        public override IObserveCommits ObserveFrom(string checkpointToken = null)
        {
            return new PollingObserveCommits(PersistStreams, _interval, null, checkpointToken);
        }

        public override IObserveCommits ObserveFromBucket(string bucketId, string checkpointToken = null)
        {
            return new PollingObserveCommits(PersistStreams, _interval, bucketId, checkpointToken);
        }

        private class PollingObserveCommits : IObserveCommits
        {
            private ILog Logger = LogFactory.BuildLogger(typeof (PollingClient));
            private readonly IPersistStreams _persistStreams;
            private string _checkpointToken;
            private readonly int _interval;
            private readonly string _bucketId;
            private readonly Subject<ICommit> _subject = new Subject<ICommit>();
            private readonly CancellationTokenSource _stopRequested = new CancellationTokenSource();
            private TaskCompletionSource<object> _runningTaskCompletionSource;
            private int _isPolling = 0;

            public PollingObserveCommits(IPersistStreams persistStreams, int interval, string bucketId, string checkpointToken = null)
            {
                _persistStreams = persistStreams;
                _checkpointToken = checkpointToken;
                _interval = interval;
                _bucketId = bucketId;
            }

            public IDisposable Subscribe(IObserver<ICommit> observer)
            {
                return _subject.Subscribe(observer);
            }

            public void Dispose()
            {
                _stopRequested.Cancel();

                int retry = 0;
                while (retry++ < 5 && Interlocked.CompareExchange(ref _isPolling, -1, 0) == 0) {
                    Logger.Info("Wait in dispose, is currently polling, retrying {RetryCount}", retry);
                    Thread.Sleep(retry * 1000);
                }

                _subject.Dispose();
                if (_runningTaskCompletionSource != null)
                {
                    _runningTaskCompletionSource.TrySetResult(null);
                }
            }

            public Task Start()
            {
                if (_runningTaskCompletionSource != null)
                {
                    return _runningTaskCompletionSource.Task;
                }
                _runningTaskCompletionSource = new TaskCompletionSource<object>();
                PollLoop();
                return _runningTaskCompletionSource.Task;
            }

            public void PollNow()
            {
                DoPoll();
            }

            private void PollLoop()
            {
                if (_stopRequested.IsCancellationRequested)
                {
                    Dispose();
                    return;
                }
                TaskHelpers.Delay(_interval, _stopRequested.Token)
                    .WhenCompleted(_ =>
                    {
                        DoPoll();
                        PollLoop();
                    },_ => Dispose());
            }

            private void DoPoll()
            {
                if (Interlocked.CompareExchange(ref _isPolling, 1, 0) == 0)
                {
                    try
                    {
                        var commits = _bucketId == null ?
                            _persistStreams.GetFrom(_checkpointToken) :
                            _persistStreams.GetFrom(_bucketId, _checkpointToken);

                        foreach (var commit in commits)
                        {
                            if (_stopRequested.IsCancellationRequested)
                            {
                                _subject.OnCompleted();
                                return;
                            }
                            _subject.OnNext(commit);
                            _checkpointToken = commit.CheckpointToken;
                        }
                    }
                    catch (Exception ex)
                    {
                        // These exceptions are expected to be transient
                        Logger.Error(ex.ToString());
                    }
                    Interlocked.Exchange(ref _isPolling, 0);
                }
            }

            private class Subject<T>
                : IObserver<T>
            {
                private readonly SemaphoreSlim subscriptionsRwLock = new SemaphoreSlim(1);
                private IList<Subscription> subscriptions =
                    new List<Subscription>();
                private static readonly IList<Subscription> disposed = new List<Subscription>(0).AsReadOnly();
                private static readonly IList<Subscription> completed = new List<Subscription>(0).AsReadOnly();
                private readonly ConcurrentQueue<Subscription> pendingUnsubscriptions =
                    new ConcurrentQueue<Subscription>();

                public IDisposable Subscribe(IObserver<T> observer)
                {
                    if (!subscriptionsRwLock.Wait(100))
                    {
                        throw new InvalidOperationException("Subscribe from within observable not supported");
                    }

                    var subscription = new Subscription(this, observer);
                    subscriptions.Add(subscription);

                    subscriptionsRwLock.Release();

                    return subscription;
                }

                public void OnNext(T item)
                {
                    try
                    {
                        subscriptionsRwLock.Wait();

                        foreach (var subscription in subscriptions)
                        {
                            subscription.Observer.OnNext(item);
                        }
                    }
                    finally
                    {
                        subscriptionsRwLock.Release();
                    }
                }

                public void OnCompleted()
                {
                    try
                    {
                        subscriptionsRwLock.Wait();
                        foreach (var subscription in subscriptions)
                        {
                            subscription.Observer.OnCompleted();
                        }
                    }
                    finally
                    {
                        subscriptionsRwLock.Release();

                        Interlocked.Exchange(ref subscriptions, completed);
                    }
                }

                public void OnError(Exception error)
                {
                    try
                    {
                        subscriptionsRwLock.Wait();

                        foreach (var subscription in subscriptions)
                        {
                            subscription.Observer.OnError(error);
                        }
                    }
                    finally
                    {
                        subscriptionsRwLock.Release();
                    }
                }

                public void Dispose()
                {
                    Interlocked.Exchange(ref subscriptions, disposed);
                }

                private void UnsubscribePending()
                {
                    if (!subscriptionsRwLock.Wait(100))
                    {
                        return;
                    }

                    try
                    {
                        while (pendingUnsubscriptions.TryDequeue(out var subscription))
                        {
                            subscriptions.Remove(subscription);
                        }
                    }
                    finally
                    {
                        subscriptionsRwLock.Release();
                    }
                }

                private void Unsubscribe(Subscription subscription)
                {
                    if (subscriptions.IsReadOnly)
                    {
                        // Instance already disposed.
                        return;
                    }

                    if (!subscriptionsRwLock.Wait(100))
                    {
                        pendingUnsubscriptions.Enqueue(subscription);
                        return;
                    }

                    try
                    {
                        subscriptions.Remove(subscription);
                    }
                    catch (System.NotSupportedException)
                    {
                        // Instance already disposed and replaced by a
                        // readonly collection but missed the IsReadOnly
                        // check due to concurrency.
                    }

                    subscriptionsRwLock.Release();
                }

                private class Subscription
                    : IDisposable
                {
                    private readonly Subject<T> subject;

                    public Subscription(Subject<T> subject, IObserver<T> observer)
                    {
                        this.subject = subject ?? throw new ArgumentNullException(nameof(subject));
                        Observer = observer ?? throw new ArgumentNullException(nameof(observer)); ;
                    }

                    public IObserver<T> Observer { get; private set; }

                    public void Dispose()
                    {
                        subject.Unsubscribe(this);
                    }
                }
            }
        }
    }
}