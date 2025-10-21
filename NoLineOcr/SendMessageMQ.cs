using RabbitMQ.Client;
using System.Text;
using System;

namespace Deduce.DMIP.NoLineOcrParse
{
    public class SendMessageMQ
    {
        /// <summary>
        /// 发送队列
        /// </summary>
        /// <param name="resource"></param>
        /// <returns></returns>
        public static void SendMessage(IModel channel, string exchangeName, string type, string routingKey, string message, byte priority)
        {
            try
            {
                var x = channel.IsOpen;
                channel.ExchangeDeclare(exchangeName, type, true, false, null);

                var body = Encoding.UTF8.GetBytes(message);
                var props = channel.CreateBasicProperties();
                props.Persistent = true;
                props.Priority = priority;


                channel.BasicPublish(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    basicProperties: props,
                    body: body);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
