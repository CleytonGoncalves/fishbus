using System;
using System.Collections.Generic;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;

namespace Thon.Hotels.FishBus
{
    public class LogCorrelationHandler
    {
        internal Func<ILogger, Message, IDisposable> PushToLogContext { get; set; }

        internal LogCorrelationHandler(bool useCorrelationLogging, LogCorrelationOptions options = null)
        {
            if (!useCorrelationLogging)
            {
                PushToLogContext = (logger, message) => new EmptyContextPusher();
            }
            else
            {
                var logPropertyName = options?.LogPropertyName ?? "CorrelationId";
                var messagePropertyName = options?.MessagePropertyName ?? "logCorrelationId";

                PushToLogContext =
                    CreatePushToLogContext(logPropertyName, messagePropertyName, options?.SetCorrelationLogId);
            }
        }

        private static Func<ILogger, Message, IDisposable> CreatePushToLogContext(string logPropertyName,
            string messagePropertyName, Action<string> setCorrelationLogId) =>
            (logger, message) =>
            {
                var logCorrelationId = message.UserProperties.ContainsKey(messagePropertyName)
                    ? message.UserProperties[messagePropertyName]
                    : Guid.NewGuid();

                setCorrelationLogId?.Invoke(logCorrelationId.ToString());
                return logger.BeginScope(new Dictionary<string, object> { { logPropertyName, messagePropertyName } });
            };
    }

    internal class EmptyContextPusher : IDisposable
    {
        public void Dispose()
        {
        }
    }
}