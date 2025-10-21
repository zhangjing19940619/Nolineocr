using Deduce.DMIP.NoLineOcrParse.Exceptions;
using DMP.Common.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Buffers;
using System.Text;
using System.Threading;

namespace Deduce.DMIP.NoLineOcrParse
{
    public class NoLineMessageHandler : RabbitMQMessageHandlerBase
    {
        private readonly NoLineService _taskService;
        private readonly CancellationTokenSource ctokenSource;

        public NoLineMessageHandler(IServiceProvider services, NoLineService taskService, ILogger<NoLineMessageHandler> logger) : base(services, logger)
        {
            _taskService = taskService;
            ctokenSource = services.GetService<CancellationTokenSource>();
        }

        public override void HandleMessage(IModel sourceChannel, string queue, BasicDeliverEventArgs eventArgs)
        {
            NoLineMetrics.CurrentTasks.Inc();
            try
            {
                if (ctokenSource.IsCancellationRequested)
                {
                    sourceChannel.BasicReject(eventArgs.DeliveryTag, true);
                    return;
                }

                var rawMessage = "";
                try
                {
                    var shared = ArrayPool<byte>.Shared;
                    var bytes = shared.Rent(eventArgs.Body.Length);
                    eventArgs.Body.CopyTo(bytes);

                    rawMessage = Encoding.UTF8.GetString(bytes, 0, eventArgs.Body.Length);

                    shared.Return(bytes, true);

                    //resource = JsonHelper.ToObject<AutoResource>(rawMessage);

                    // set logger props
                    //LogicalThreadContext.Properties.Set(CommonLoggingConsts.PROPNAME_RESOURCEID, resource.ObjectID ?? string.Empty);

                    Logger.LogInformationWithProps(new { type = "获取队列 " + queue }, rawMessage);
                }
                catch (Exception ex)
                {
                    throw new UnrecoverableException($"消息解析异常", ex); // 消息解析异常无法恢复
                }

                if (!_taskService.Execute(queue, rawMessage))
                {
                    Logger.LogWarning("消费失败, 消息退回死信队列");
                    sourceChannel.BasicReject(eventArgs.DeliveryTag, false);

                    // Basic Recover 重入队列，并优先由其他消费者处理
                    //sourceChannel.BasicRecover(true);
                }
                else
                {
                    sourceChannel.BasicAck(eventArgs.DeliveryTag, false);
                }
            }
            catch (UnrecoverableException ex)
            {
                Logger.LogErrorWithProps(new { type = "入库异常" }, ex, "入库发生不可恢复异常");
                sourceChannel.BasicAck(eventArgs.DeliveryTag, false);
            }
            catch (RecoverableException ex)
            {
                Logger.LogWarningWithProps(new { type = "入库异常" }, ex, "入库发生可恢复异常, 消息退回死信队列");
                sourceChannel.BasicReject(eventArgs.DeliveryTag, !queue.EndsWith("-dlx"));
            }
            catch (Exception ex)
            {
                Logger.LogErrorWithProps(new { type = "入库异常" }, ex, "入库发生异常");
                sourceChannel.BasicReject(eventArgs.DeliveryTag, !queue.EndsWith("-dlx"));
            }
            finally
            {
                NoLineMetrics.CurrentTasks.Dec();
            }
        }
    }
}
