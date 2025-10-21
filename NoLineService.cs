using Deduce.DMIP.NoLineOcrParse.NoLineOcr;
using Deduce.DMIP.ResourceManage;
using DMP.Common.Caching.Caches;
using DMP.Common.RabbitMQ;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Deduce.DMIP.NoLineOcrParse
{
    public class NoLineService
    {
        private const string NAME_RABBITMQ_DMP = "dmp-189";
        private readonly IRabbitMQConnectionFactory _rabbitMQConnectionFactory;
        private readonly ILogger _logger;
        private string _queueName = "";
        private readonly IModel _channel;
        private readonly NoLineHelper _noLineHelper;
        private static Timer queryTimer;

        private readonly ICommonRedisCache _commonRedisCache;

        public NoLineService(ICommonRedisCache commonRedisCache, IRabbitMQConnectionFactory rabbitMQConnectionFactory, NoLineHelper noLineHelper, ILogger<NoLineService> logger)
        {
            _rabbitMQConnectionFactory = rabbitMQConnectionFactory;
            _logger = logger;
            _commonRedisCache = commonRedisCache;
            _noLineHelper = noLineHelper;
            // 初始化定时器，每隔30秒执行一次查询方法
            queryTimer = new Timer(ExecuteQuery, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));
        }

        public bool Execute(string queueName, string msg)
        {           
            //消费已解析的数据
            return _noLineHelper.Execute(msg);           
        }

        /// <summary>
        /// 定时执行的查询方法
        /// </summary>
        /// <param name="state">定时器回调的状态参数</param>
        private void ExecuteQuery(object state)
        {
            try
            {
                //推送解析
                _noLineHelper.ExecuteRun();
            }
            catch (Exception ex)
            {
                ConsoleRegion.WriteToBuffer(true, $"推送解析出错: {ex.Message}");               
            }
        }
    }
}