using Deduce.Common.Components;
using Deduce.Common.Utility;
using Deduce.DMIP.Business.Components;
using Deduce.DMIP.Sys.OperateData;
using DMP.Common.Caching.Caches;
using DMP.Common.RabbitMQ;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Deduce.DMIP.NoLineOcrParse.NoLineOcr
{
    public class NoLineHelper
    {
        static OperateData _data = OperateData.Instance;
        /// <summary>
        /// 需要转换的队列
        /// </summary>
        ConcurrentDictionary<string, DataRow> _convertQueues = new ConcurrentDictionary<string, DataRow>();

        /// <summary>
        /// 正在执行的队列
        /// </summary>
        ConcurrentDictionary<string, DataRow> _runQueues = new ConcurrentDictionary<string, DataRow>();

        private readonly ICommonRedisCache _commonRedisCache;
        private readonly ILogger _logger;
        private readonly NoLineToXmlQueue _noLineToXmlQueue;
        private readonly IModel _publishChannel;
        public NoLineHelper(ICommonRedisCache commonRedisCache, IRabbitMQConnectionFactory rabbitMQConnectionFactory, NoLineToXmlQueue noLineToXmlQueue, ILogger<NoLineHelper> logger)
        {
            _commonRedisCache = commonRedisCache;
            _logger = logger;
            _publishChannel = rabbitMQConnectionFactory.GetOrCreatePublishModel(CommonRabbitMQConsts.CONNECTION_DMP_189);
            _noLineToXmlQueue = noLineToXmlQueue;
        }

        public static string[] GetModuleID()
        {
            //string path = Utils.GetEtcPath + "pdfCutMoudle.txt";
            //if (!File.Exists(path))
            //    return null;

            //string txtContent = Utils.ReadTxtOrCsv(path);
            //return txtContent.Replace("\r\n", "\n").Split('\n');

            var sql = "SELECT moduleID FROM cfg.dmip_ArchiveRule(nolock) WHERE noLineXmlType IN(1,2)";
            DataTable dt = _data.GetDataTable(sql, GlobalData.CommonMenuID);

            // 处理空结果集
            if (dt == null || dt.Rows.Count == 0)
                return Array.Empty<string>(); // 返回空数组优于null，避免调用方NPE

            // 转换DataTable到字符串数组
            var result = new List<string>(dt.Rows.Count);
            foreach (DataRow row in dt.Rows)
            {
                // 安全处理DBNull和空值
                object value = row["moduleID"];
                result.Add(value == DBNull.Value || value == null
                    ? string.Empty
                    : value.ToString().Trim());
            }

            return result.ToArray();
        }
        //获取待无线表格解析的公告
        public static DataTable GetNoLineWaitPdf(string resourceId = "", string moduleId = "", string date = "")
        {
            //resourceId = @"('bb6ac01ad111dec28fd882d54e8e7a81')";
            int interval = ServiceSetting.MaxTasks;
            string startDate = Utils.IsEmpty(date) ? DateTime.Now.AddDays(-interval).ToString(GlobalData.DateFormat) : date;
            string[] moduleIds = GetModuleID();
            DataTable dt = new DataTable();
            string moduleIdStr = string.Join(", ", moduleIds.Select(s => $"'{s}'"));
            ConsoleRegion.WriteToBuffer(true, "本次查询节点数量："+moduleIds.Count());
            GetDataTable(dt,moduleIdStr, startDate);
            
            //获取重置的资源
            string sql = @"select a.moduleID,a.ob_object_id as resourceID,a.xmlType AS noLineXmlType,d.igsdm,
                           d.ggrq,d.resMD5,d.title,d.storePath,a.createTime,a.isPriority 
                           FROM cfg.dmip_ResourceStatus(nolock) a 
                           inner join cfg.dmip_Resource(nolock) d on a.ob_object_id = d.ob_object_id 
                           WHERE a.isPriority ='6' and a.moduleID in ("+ moduleIdStr + ")  ORDER BY a.createTime ";

            DataTable dtTemp = _data.GetDataTable(sql, GlobalData.CommonMenuID);

            //   //获取超时(解析超过5分钟)未解析成功的资源
            //   string sqlTimeOut = @"select top 200 a.moduleID,a.ob_object_id as resourceID,a.xmlType AS noLineXmlType,d.igsdm,
            //                  d.ggrq,d.resMD5,d.title,d.storePath,a.createTime,a.isPriority 
            //                  FROM cfg.dmip_ResourceStatus(nolock) a 
            //                  inner join cfg.dmip_Resource(nolock) d 
            //on a.ob_object_id = d.ob_object_id 
            //                  WHERE a.isPriority ='1' and DATEDIFF(MINUTE, a.createTime, GETDATE()) > 5
            //ORDER BY a.createTime desc ";

            //   DataTable dtTimeOut = _data.GetDataTable(sqlTimeOut, GlobalData.CommonMenuID);
            dt.Merge(dtTemp);
            //dt.Merge(dtTimeOut);
            #region//测试SQL代码
            //sql = @"select top 10 a.moduleID,a.resourceID,b.noLineXmlType,d.igsdm,d.ggrq,d.resMD5,c.fromPath,d.title    from cfg.dmip_ArchiveData(nolock) a
            //               inner join cfg.dmip_ArchiveRule(nolock) b on a.moduleID=b.moduleID
            //               inner join cfg.dmip_ConvertHashCode(nolock) c on a.resourceID=c.resourceID and c.Status=0
            //               inner join cfg.dmip_Resource(nolock) d on c.ob_object_id=d.ob_object_id 
            //               where a.resourceID='bb6ac01ad111dec28fd882d54e8e7a81'  
            //               and b.noLineXmlType>0 ";
            #endregion

            //if (dt != null && dt.Rows.Count > 0)
            //return dt;

            //dt = GetSpecialData(resourceId, moduleIds, startDate);

            return dt;
        }

        /// <summary>
        /// 获取数据表并处理超时任务。
        /// </summary>
        /// <param name="dt">要合并的数据表。</param>
        /// <param name="moduleId">模块ID。</param>
        /// <param name="startDate">开始日期。</param>
        private static void GetDataTable(DataTable dt, string moduleIdStr, string startDate)
        {
            DataTable dtTemp = FetchPendingResources(moduleIdStr, startDate);
            ResetTimeoutTasks(moduleIdStr, startDate);

            if (!Utils.IsEmpty(dtTemp))
            {
                dt.Merge(dtTemp);
            }
        }

        /// <summary>
        /// 获取待解析的资源。
        /// </summary>
        /// <param name="moduleId">模块ID。</param>
        /// <param name="startDate">开始日期。</param>
        /// <returns>返回待解析的资源数据表。</returns>
        private static DataTable FetchPendingResources(string moduleIdStr, string startDate)
        {
            string sql = @"select top " + ServiceSetting.MinTasks + @" a.moduleID,a.resourceID,b.noLineXmlType,d.igsdm,
                           d.ggrq,d.resMD5,d.title,d.storePath,a.createTime,'' AS isPriority   
                           from cfg.dmip_ArchiveData(nolock) a
                           inner join cfg.dmip_ArchiveRule(nolock) b on a.moduleID=b.moduleID and b.noLineXmlType > 0 
                           inner join cfg.dmip_Resource(nolock) d on a.resourceID = d.ob_object_id 
                           where a.moduleID in (" + moduleIdStr + ")  and a.createTime >='" + startDate + @"'
                           and a.resourceID not in (select ob_object_id from cfg.dmip_ResourceStatus(nolock)) 
                           ORDER BY a.createTime ";

            return _data.GetDataTable(sql);
        }

        /// <summary>
        /// 重置超过5分钟未解析成功的任务。
        /// </summary>
        /// <param name="moduleId">模块ID。</param>
        /// <param name="startDate">开始日期。</param>
        private static void ResetTimeoutTasks(string moduleIdStr, string startDate)
        {
            string sqlTimeOut = @"select top 200 * from cfg.dmip_ResourceStatus(nolock) where isPriority = 1 and moduleID in  (" + moduleIdStr + ")  " +
                " and createTime >='" + startDate + "' and DATEDIFF(MINUTE, createTime, GETDATE()) > 5";

            DataTable dtTimeOut = _data.GetDataTable(sqlTimeOut, GlobalData.CommonMenuID);

            if (!Utils.IsEmptyTable(dtTimeOut))
            {
                foreach (DataRow dr in dtTimeOut.Rows)
                {
                    dr["status"] = 6;
                    dr["isPriority"] = 6;
                    dr["createTime"] = DateTime.Now;
                }

                bool success = _data.DataImport("cfg.dmip_ResourceStatus", dtTimeOut, ModifyType.Update, GlobalData.CommonMenuID);
                if (success)
                {
                    Utils.WriteInfoLog("重置超时5分钟的任务");
                }
            }
        }

        private bool LoadParserData()
        {
            try
            {
                DataTable dt = GetNoLineWaitPdf();
                if (Utils.IsEmpty(dt))
                    return false;

                foreach (DataRow dr in dt.Rows)
                {
                    string resourceID = dr["resourceID"].ToString();
                    if (_convertQueues.ContainsKey(resourceID))
                        continue;

                    _convertQueues.TryAdd(resourceID, dr);
                }
                return true;
            }
            catch (Exception ex)
            {
                Utils.WriteLog("LoadConvertData 异常错误！" + ex.Message);
                DingDing.Send("无线表格解析异常通知：LoadConvertData异常", ex.Message + ex.StackTrace, 0, "");
            }
            return false;
        }
        public void ExecuteRun()
        {
            //DisplayMsg("获取待推送公告信息...");
            //加载资源
            LoadParserData();
            if (_convertQueues == null || _convertQueues.Count == 0)
            {
                ConsoleRegion.WriteToBuffer(true, "本次无公告需要推送至消息队列...");
                return;
            }

            ConsoleRegion.WriteToBuffer(true, "当前共有：" + _runQueues.Count + " 条公告正在推送消息队列，请等待...");

            foreach (var queues in _convertQueues)
            {
                if (Utils.IsEmpty(queues.Key) || _runQueues.ContainsKey(queues.Key))
                    continue;
                //增加执行线程数
                SetTreadNum(true, queues.Key);
                Thread t = new Thread(() =>
                {
                    Execute(queues.Value);
                    SetTreadNum(false, queues.Key);
                });
                t.IsBackground = true;
                t.SetApartmentState(ApartmentState.STA);
                t.Start();

            }
        }

        private void Execute(DataRow rd)
        {
            _noLineToXmlQueue.ProducerExecute(rd, out string pdfMsg);

        }

        public bool Execute(string msg)
        {
            bool isEnd = false;
            string pdfMsg = "";
            Thread t = new Thread(() =>
            {
                _noLineToXmlQueue.ConsumerExecute(msg, out pdfMsg);
                isEnd = true;
            });
            t.IsBackground = true;
            t.SetApartmentState(ApartmentState.STA);
            t.Start();

            ConsoleRegion.WriteToBuffer(false, "正在消费解析完成队列中的公告，请等待...");
            while (!isEnd)
            {
                Thread.Sleep(1000 * 2);
            }

            if (Utils.IsEmpty(pdfMsg))
                return true;

            return false;
        }

        /// <summary>
        /// 设置线程数，移除运行队列
        /// </summary>
        /// <param name="add"></param>
        private void SetTreadNum(bool add, string key = "")
        {
            if (!_convertQueues.Keys.Contains(key))
                return;

            var data = _convertQueues[key];
            if (add)
            {
                _runQueues.TryAdd(key, data);
                return;
            }
            _runQueues.TryRemove(key, out data);
            _convertQueues.TryRemove(key, out data);
        }

        public void DisplayMsg(string msg)
        {
            Console.WriteLine($"推送{DateTime.Now}：{msg}");
        }

        public void DisplayMsg2(string msg)
        {
            Console.WriteLine($"消费{DateTime.Now}：{msg}");
        }
    }
}
