namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using NEventStore.Logging;

    public class ThreadScope<T> : IDisposable where T : class
    {
        [ThreadStatic]
        private static Dictionary<string, T> _context;

        private static AsyncLocal<Dictionary<string, T>> _asyncLocalContext;

        private readonly T _current;
        private readonly ILog _logger = LogFactory.BuildLogger(typeof (ThreadScope<T>));
        private readonly bool _rootScope;
        private readonly string _threadKey;
        private bool _disposed;

        public ThreadScope(string key, Func<T> factory)
        {
            _threadKey = typeof (ThreadScope<T>).Name + ":[{0}]".FormatWith(key ?? string.Empty);

            T parent = Load();
            _rootScope = parent == null;
            _logger.Debug(Messages.OpeningThreadScope, _threadKey, _rootScope);

            _current = parent ?? factory();

            if (_current == null)
            {
                throw new ArgumentException(Messages.BadFactoryResult, "factory");
            }

            if (_rootScope)
            {
                Store(_current);
            }
        }

        public T Current
        {
            get { return _current; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            _logger.Debug(Messages.DisposingThreadScope, _rootScope);
            _disposed = true;
            if (!_rootScope)
            {
                return;
            }

            _logger.Verbose(Messages.CleaningRootThreadScope);
            Store(null);

            var resource = _current as IDisposable;
            if (resource == null)
            {
                return;
            }

            _logger.Verbose(Messages.DisposingRootThreadScopeResources);
            resource.Dispose();
        }

        private T Load()
        {
            if (_context != null && _context.TryGetValue(_threadKey, out T value))
            {
                return value;
            }

            return null;
        }

        private void Store(T value)
        {
            if (_asyncLocalContext == null)
            {
                Interlocked.CompareExchange(
                    ref _asyncLocalContext,
                    new AsyncLocal<Dictionary<string, T>>(AsyncLocalSetContext),
                    null);
                _asyncLocalContext.Value = new Dictionary<string, T>();
            }

            if (_context == null) {
                _asyncLocalContext.Value = new Dictionary<string, T>();
            }

            if (_context == null) {
                throw new InvalidOperationException();
            }

            _context[_threadKey] = value;
        }

        private static void AsyncLocalSetContext(AsyncLocalValueChangedArgs<Dictionary<string, T>> args)
        {
            _context = args.CurrentValue;
        }
    }
}
