using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Thon.Hotels.FishBus
{
    /// The main/singleton service that contains all messaging clients
    public class MessagingService : IHostedService
    {
        private MessagingConfiguration Configuration { get; }

        private ILogger<MessagingService> Logger { get; }

        public MessagingService(MessagingConfiguration configuration)
            : this(configuration, NullLogger<MessagingService>.Instance)
        {
        }

        public MessagingService(MessagingConfiguration configuration, ILogger<MessagingService> logger)
        {
            Configuration = configuration;
            Logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                Configuration.RegisterMessageHandlers(ExceptionReceivedHandler);
                return Task.CompletedTask;
            }
            catch (Exception exception)
            {
                Logger.LogError($"Error registering message handler", exception);
                return Task.CompletedTask;
            }
        }

        Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Logger.LogError(exceptionReceivedEventArgs.Exception,
                        @"Message handler encountered an exception.
                        Endpoint: {Endpoint}
                        Entity Path: {EntityPath}
                        Executing Action: {Action}",
                        context.Endpoint, context.EntityPath, context.Action);

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("Signal received. Gracefully shutting down.");
            await Configuration.Close();
            Thread.Sleep(1000);
        }
    }
}
