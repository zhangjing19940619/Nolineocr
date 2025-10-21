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
                // ����Ƿ�������ض�����û�в�ִ������
                if (!Console.IsOutputRedirected)
                {
                    Console.Clear();
                }               
                bootStrapper = new AutofacOwinBootStrapper(args);

                await bootStrapper.StartServiceAsync();
                ConsoleRegion.WriteToBuffer(true, "���߱�������������......");
                ConsoleRegion.WriteToBuffer(true, "��󲢷���(MaxThread)��" + ServiceSetting.MaxThreadCount);
                ConsoleRegion.WriteToBuffer(true, "���������(MinTasks)��" + ServiceSetting.MinTasks);
                ConsoleRegion.WriteToBuffer(true, "�������ڷ�Χ(MaxTasks)��" + ServiceSetting.MaxTasks + "����");
                ConsoleRegion.WriteToBuffer(true, "����ڵ�(pdfCutMoudle)��" + String.Join(", ", NoLineHelper.GetModuleID()));
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
