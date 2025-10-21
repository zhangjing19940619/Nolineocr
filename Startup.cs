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
    /// Ӧ��������
    /// ע������˳��
    /// </summary>
    [AssemblyStartupDependency(
         typeof(DMP.Common.Configuration.Startup)    // �����������
        , typeof(DMP.Common.Logging.Startup)        // ��׼����־������������� DMP.Common.Configuration.Startup ֮��
                                                    //, typeof(Auto.Common.DBSConnect.Startup)    // DBS ����������������� DMP.Common.Configuration.Startup �� DMP.Common.Logging.Startup ֮��
        , typeof(SkyApm.Agent.Owin.Startup)         // Owin SelfHost Tracing���������� SkyApm.Tracing.RabbitMQ.Startup �� SkyApm.Tracing.Quartz.Startup ֮ǰ��
        , typeof(DMP.Common.HttpApis.Startup)       // HttpApis
        , typeof(Deduce.DMIP.Business.Components.Startup)       // HttpApis
        , typeof(SkyApm.Tracing.Http.Startup)       // Http Tracing(�������� DMP.Common.HttpApis.Startup ֮��  )
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
                throw new InvalidOperationException($"���� DBS ʧ��");
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

            Console.Title = "���߱��������ѷ���";
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
