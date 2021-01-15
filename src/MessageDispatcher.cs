using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;

namespace Thon.Hotels.FishBus
{
    public class MessageDispatcher
    {
        private LogCorrelationHandler LogCorrelationHandler { get; }

        private IReceiverClient Client { get; }

        private IServiceScopeFactory ScopeFactory { get; }

        private MessageHandlerRegistry Registry { get; }

        private ILogger<MessageDispatcher> Logger { get; }

        internal MessageDispatcher(IServiceScopeFactory scopeFactory, IReceiverClient client, MessageHandlerRegistry registry, LogCorrelationHandler logCorrelationHandler)
            : this(scopeFactory, client, registry, logCorrelationHandler, NullLogger<MessageDispatcher>.Instance)
        {
        }

        internal MessageDispatcher(IServiceScopeFactory scopeFactory, IReceiverClient client, MessageHandlerRegistry registry, LogCorrelationHandler logCorrelationHandler, ILogger<MessageDispatcher> logger)
        {
            LogCorrelationHandler = logCorrelationHandler;
            ScopeFactory = scopeFactory;
            Client = client;
            Registry = registry;
            Logger = logger;
        }

        // Call all handlers for the message type given by the message label.
        // There can be multiple handlers per message type
        public async Task ProcessMessage(Message message, CancellationToken token)
        {
            using (LogCorrelationHandler.PushToLogContext.Invoke(Logger, message))
            {
                var body = Encoding.UTF8.GetString(message.Body);
                var sw = Stopwatch.StartNew();
                Logger.LogDebug("Received message: SequenceNumber: {SequenceNumber} Body: {Body}", message.SystemProperties.SequenceNumber, body);
                try
                {
                    if (!string.IsNullOrWhiteSpace(message.Label))
                    {
                        await ProcessMessage(message.Label, body,
                            () => Client.CompleteAsync(message.SystemProperties.LockToken),
                            m => AddToDeadLetter(message.SystemProperties.LockToken, m));
                    }
                    else
                    {
                        Logger.LogError("Message label is not set. \n Message: {Body} \n Forwarding to DLX", body);
                        await AddToDeadLetter(message.SystemProperties.LockToken, "Message label is not set.");
                    }
                }
                catch (JsonException jsonException)
                {
                    Logger.LogError(jsonException,
                        "Unable to deserialize message. \n Message: {Body} \n Forwarding to DLX", body);
                    await AddToDeadLetter(message.SystemProperties.LockToken, jsonException.Message);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Caught exception while handling message with label {Label} and body {Body}",
                        message.Label, body);
                    throw;
                }

                Logger.LogDebug("Completed handling message in {HandlingTime} ms", sw.ElapsedMilliseconds);
            }
        }

        internal async Task ProcessMessage(string label, string body, Func<Task> markCompleted, Func<string, Task> abort)
        {
            var typeFromLabel = Registry.GetMessageTypeByName(label);
            if (typeFromLabel != default)
            {
                var message = JsonConvert.DeserializeObject(body, typeFromLabel);

                using (var scope = ScopeFactory.CreateScope())
                {
                    var tasks = Registry
                                    .GetHandlers(message.GetType(), scope)
                                    .ToList() // avoid deferred execution, we want all handlers to execute
                                    .Select(h => CallHandler(h, message, abort))
                                    .ToArray();
                    var results = await Task.WhenAll(tasks);
                    if (results.All(r => r))
                        await markCompleted();
                }
            }
            else
            {
                Logger.LogDebug("No handler registered for the given {Label}. {Body}", label, body);
                await markCompleted();
            }
        }

        private async Task<bool> CallHandler(object handler, object message, Func<string, Task> abort)
        {
            var tasks =
                handler
                    .GetType()
                    .GetMethods()
                    .Where(m => HandlesMessageOfType(m, message.GetType()))
                    .Select(m => (Task<HandlerResult>)m.Invoke(handler, new[] { message }))
                    .ToArray();
            if (!tasks.Any())
                return true;

            if (tasks.Length > 1)
                Logger.LogWarning("More than one method named Handle in type: {HandlerType}", handler.GetType().FullName);

            var results = await Task.WhenAll(tasks);
            var aborted = results.FirstOrDefault(HandlerResult.IsAbort);
            if (aborted != null)
            {
                await abort(aborted.Message);
                return false;
            }
            return !results.Any(HandlerResult.IsFailed);
        }

        private bool HandlesMessageOfType(MethodInfo m, Type type)
        {
            var parameters = m.GetParameters();
            if (m.Name != "Handle" || !parameters.Any()) return false;
            return parameters[0].ParameterType == type;
        }

        internal async Task Close()
        {
            await Client.CloseAsync();
        }

        internal void RegisterMessageHandler(Func<ExceptionReceivedEventArgs, Task> exceptionReceivedHandler)
        {
            Client.RegisterMessageHandler(ProcessMessage, new MessageHandlerOptions(exceptionReceivedHandler)
            {
                AutoComplete = false,
            });
        }

        private async Task AddToDeadLetter(string lockToken, string errorMessage)
        {
            await Client.DeadLetterAsync(lockToken, "Invalid message", errorMessage);
        }
    }
}
