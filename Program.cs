using Deduce.Common.Entity;
using Deduce.DMIP.Business.Components;
using Deduce.DMIP.NoLineOcrParse.NoLineOcr;
using Deduce.DMIP.ResourceManage;
using DMP.Common.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Deduce.DMIP.NoLineOcrParse
{
    internal class Program
    {
        internal static IBootStrapper bootStrapper;
        [STAThread]
        public static async Task Main(string[] args)
        {
            try
            {
                // 检查是否有输出重定向，若没有才执行清屏
                if (!Console.IsOutputRedirected)
                {
                    Console.Clear();
                }               
                bootStrapper = new AutofacOwinBootStrapper(args);

                await bootStrapper.StartServiceAsync();
                ConsoleRegion.WriteToBuffer(true, "无线表格解析服务启动......");
                ConsoleRegion.WriteToBuffer(true, "最大并发数(MaxThread)：" + ServiceSetting.MaxThreadCount);
                ConsoleRegion.WriteToBuffer(true, "最大任务数(MinTasks)：" + ServiceSetting.MinTasks);
                ConsoleRegion.WriteToBuffer(true, "任务日期范围(MaxTasks)：" + ServiceSetting.MaxTasks + "天内");
                ConsoleRegion.WriteToBuffer(true, "任务节点(pdfCutMoudle)：" + String.Join(", ", NoLineHelper.GetModuleID()));
                Console.CancelKeyPress += new ConsoleCancelEventHandler((sender, arg) => ConsoleCancelHandler(bootStrapper, sender, arg));
                               
                while (true)
                {
                    Console.Read();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                //Environment.Exit(-1);
            }
        }

        protected static void ConsoleCancelHandler(IBootStrapper bootStrapper, object sender, ConsoleCancelEventArgs args)
        {
            bootStrapper.ShutdownAsync(null).Wait();
        }
    }
}
