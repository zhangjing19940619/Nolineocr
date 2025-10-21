using Autofac;
using Autofac.Integration.WebApi;
using Deduce.Common.Entity;
using Deduce.Common.Utility;
using Deduce.DMIP.Business.Components;
using Deduce.DMIP.NoLineOcrParse.NoLineOcr;
using Deduce.DMIP.ResourceManage;
using Deduce.DMIP.Sys.OperateData;
using DMP.Common.Framework;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Deduce.DMIP.NoLineOcrParse.Tests")]
namespace Deduce.DMIP.NoLineOcrParse
{
    /// <summary>
    /// 应用启动类
    /// 注意启动顺序
    /// </summary>
    [AssemblyStartupDependency(
         typeof(DMP.Common.Configuration.Startup)    // 公共配置组件
        , typeof(DMP.Common.Logging.Startup)        // 标准化日志组件（必须列在 DMP.Common.Configuration.Startup 之后）
                                                    //, typeof(Auto.Common.DBSConnect.Startup)    // DBS 连接组件（必须列在 DMP.Common.Configuration.Startup 和 DMP.Common.Logging.Startup 之后）
        , typeof(SkyApm.Agent.Owin.Startup)         // Owin SelfHost Tracing（必须列在 SkyApm.Tracing.RabbitMQ.Startup 与 SkyApm.Tracing.Quartz.Startup 之前）
        , typeof(DMP.Common.HttpApis.Startup)       // HttpApis
        , typeof(Deduce.DMIP.Business.Components.Startup)       // HttpApis
        , typeof(SkyApm.Tracing.Http.Startup)       // Http Tracing(必须列在 DMP.Common.HttpApis.Startup 之后  )
        , typeof(SkyApm.Tracing.RabbitMQ.Startup)   // RabbitMQ & Tracing
                                                    //, typeof(SkyApm.Tracing.Quartz.Startup)     // Quartz & Tracing
                                                    //, typeof(DMP.Common.CrystalQuartz.Startup)  // CrystalQuartz
        )]

    public class Startup : OwinStartupBase, IAutofacStartup
    {        
        public Startup(IHostEnvironment hostEnvironment) : base(hostEnvironment)
        {
        }

        [STAThread]
        public override IConfigurationBuilder BuildConfiguration(IConfigurationBuilder configurationBuilder)
        {
            ResultData<bool> result = AutoHelper.DataService.Login(ServiceSetting.City, "");
            if (!result.Result)
            {
                throw new InvalidOperationException($"连接 DBS 失败");
            }
            Utils.CheckUse();
            return base.BuildConfiguration(configurationBuilder);
        }

        public override IServiceCollection ConfigureServices(IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<NoLineMessageHandler>();           
            services.AddSingleton<NoLineService>();
            services.AddSingleton<NoLineHelper>();
            services.AddSingleton<NoLineOcr.NoLineToXmlQueue>();
            return base.ConfigureServices(services, configuration);
        }

        public override async Task StartServiceAsync(IServiceProvider services, IConfiguration configuration, CancellationTokenSource applicationCancellationTokenSource)
        {
            var logger = services.GetService<ILoggerFactory>().CreateLogger<Startup>();

            Console.Title = "无线表格解析消费服务";
            var rabbitNotifyFactory = (IRabbitNotifyFactory)services.GetService(typeof(IRabbitNotifyFactory));
            RabbitNotify.Setup(rabbitNotifyFactory);
            var redisHelperFactory = (IRedisProxy)services.GetService(typeof(IRedisProxy));
            RedisHelper.Setup(redisHelperFactory);            
            await base.StartServiceAsync(services, configuration, applicationCancellationTokenSource);
        }

        public ContainerBuilder ConfigureAutofacServices(ContainerBuilder containerBuilder)
        {
            containerBuilder.RegisterApiControllers(typeof(Startup).Assembly);

            return containerBuilder;
        }
    }
}
