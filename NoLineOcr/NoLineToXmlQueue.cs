using Deduce.Common.Components;
using Deduce.Common.Entity;
using Deduce.Common.PdfViewer;
using Deduce.Common.Utility;
using Deduce.DMIP.Business.Components;
using Deduce.DMIP.ResourceManage;
using Deduce.DMIP.ResourceManage.Service.NoLineToXml;
using Deduce.DMIP.Sys.OperateData;
using Deduce.DMIP.Sys.SysData;
using DMP.Common.Caching.Caches;
using DMP.Common.RabbitMQ;
using IntellectOcr;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly.Caching;
using RabbitMQ.Client;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;

namespace Deduce.DMIP.NoLineOcrParse.NoLineOcr
{
    public class NoLineToXmlQueue
    {
        static OperateData _data = OperateData.Instance;
        //static LogServerHelper //_logServer ;
        string _logType = "无线解析流程";
        //string parseTemplate = "PDF_J";
        Dictionary<string, string> templateMap;
        bool isSendTask = true;
        ResData _resData;
        private readonly ICommonRedisCache _commonRedisCache;
        private readonly ILogger _logger;
        private readonly IModel _dmpaiChannel;
        private readonly IModel _dmpChannel;
        public NoLineToXmlQueue(ICommonRedisCache commonRedisCache, IRabbitMQConnectionFactory rabbitMQConnectionFactory, ILogger<NoLineToXmlQueue> logger, IConfiguration configuration)
        {
            _commonRedisCache = commonRedisCache;
            _logger = logger;
            _dmpaiChannel = rabbitMQConnectionFactory.GetOrCreatePublishModel("dmp-ai-189");
            _dmpChannel = rabbitMQConnectionFactory.GetOrCreatePublishModel(CommonRabbitMQConsts.CONNECTION_DMP_189);
            templateMap = configuration.GetSection("noLineOcrParse:templateMap").Get<Dictionary<string, string>>(); ;
            bool.TryParse((configuration["noLineOcrParse:isSendTask"] ?? "true"), out isSendTask);
        }

        #region 消息推送
        /// <summary>
        /// 生产者执行入口
        /// </summary>
        public void ProducerExecute(DataRow rd, out string pdfMsg)
        {
            pdfMsg = "";
            string resourceId = rd["resourceID"].ToString();
            string moduleId = rd["moduleID"].ToString();
            string fromPath = rd["storePath"].ToString();
            string title = rd["title"].ToString();
            string resourceInfo = resourceId + "_" + title;
            int.TryParse(rd["isPriority"].ToString(), out int priority);
            //frmViewer frmPicture = new frmViewer();

            try
            {
                bool isNum = int.TryParse(rd["noLineXmlType"]?.ToString(), out int noLineXmlType);
                if (!isNum || noLineXmlType == 0)
                {
                    pdfMsg = "当前节点没有配置无线解析";
                    return;
                }

                //查看是否解析
                if (IsNoLineRecord(resourceId, priority))
                    return;

                ConsoleRegion.WriteToBuffer(true, "当前正在推送：" + resourceInfo);
                ////_logServer.Info(_logType, "当前正在推送：" + resourceInfo, resourceId);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = resourceId }, "当前正在推送：" + resourceInfo);
                if (!RCFastDfs.Init.Exist(resourceId).Success)
                {
                    ////_logServer.Info(_logType, "资源中心不存在文件，结束处理：" + resourceInfo, resourceId);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = resourceId }, "资源中心不存在文件，结束处理：" + resourceInfo);
                    return;
                }

                //保存解析记录
                if (!InsertNoLineRecord(resourceId, moduleId, rd, noLineXmlType, priority))
                    return;

                if (priority == (int)IsPriority.ResetRes)
                    UpdateNoLineRecord(resourceId, IsPriority.ResetRes);

                //检查测试环境资源 没有从正式环境同步测试环境
                CheckResource(resourceId, fromPath);

                ////_logServer.Info(_logType, "下载PDF文件." + resourceInfo, resourceId);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = resourceId }, "下载PDF文件." + resourceInfo);
                //下载pdf
                ResultData<string> resultPdf = RCFastDfs.Init.DownLoad(resourceId);
                if (!resultPdf.Success || Utils.IsEmpty(resultPdf.Result))
                {
                    ConsoleRegion.WriteToBuffer(true, "下载pdf失败：" + resourceInfo + " , 错误信息为：" + resultPdf.Error);
                    UpdateNoLineRecord(resourceId, IsPriority.PdfDownFail, 0, 0, 1);
                    DingDing.Send("无线表格解析异常通知", "从fast中下载公告失败", 0, resourceInfo + " , 错误信息为：" + resultPdf.Error);
                    pdfMsg = "从fast中下载公告失败,解析失败";
                    return;
                }

                //检查PDF是否能正常打开
                bool isOpen = NoLineToXmlHelper.Open(resultPdf.Result, out int pdfNum, resourceId);
                if (!isOpen)
                {
                    UpdateNoLineRecord(resourceId, IsPriority.PdfDamage, pdfNum, 0, 1);
                    DingDing.Send("无线表格异常通知", "公告异常无法打开", 0, resourceInfo);
                    pdfMsg = "公告异常无法打开,解析失败";
                    return;
                }

                //保存解析记录
                UpdateNoLineRecord(resourceId, IsPriority.InProcess, pdfNum);

                //推送消息
                if (PushResToQueue(resourceId, resourceInfo, (NoLineType)noLineXmlType, ref pdfMsg))
                    return;

                DingDing.Send("无线表格解析异常通知", "推送消息队列失败", 0, resourceInfo + " , 错误信息为：" + pdfMsg);
                UpdateNoLineRecord(resourceId, IsPriority.PushError, pdfNum, 0);

            }
            catch (Exception ex)
            {
                DingDing.Send("无线表格解析异常通知：NoLineToXmlQueue推送异常", ex.Message + ex.StackTrace, 0, resourceInfo);
                pdfMsg = "无线表格解析异常通知";
                UpdateNoLineRecord(resourceId, IsPriority.ParserError, 0, 0, 1);
            }
            finally
            {
                //Dispose(frmPicture,null, resourceInfo, true);
            }
        }

        /// <summary>
        /// 推送消息到队列
        /// </summary>
        private bool PushResToQueue(string resourceId, string resourceInfo, NoLineType xmlType, ref string msg)
        {
            //_logServer.Info(_logType, "开始执行推送: " + resourceInfo, resourceId);
            _logger.LogInformationWithProps(new { type = _logType, resourceId = resourceId }, "开始执行推送: " + resourceInfo);
            try
            {
                var dataBuffer = BuildDataBuffer(resourceId, xmlType.ToString().Contains("股票"), xmlType);
                var exchangeName = "dmp-to-ai-file-convert-exchange";
                var exchangeType = "topic";
                var routingKey = "dmp-file-convert.*";
                var priority = "0";
                SendMessageMQ.SendMessage(_dmpaiChannel, exchangeName, exchangeType, routingKey, dataBuffer, Convert.ToByte(priority));
                //_logServer.Info(_logType, "推送成功：" + dataBuffer, resourceId);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = resourceId }, "推送成功：" + dataBuffer);
                return true;
            }
            catch (Exception ex)
            {
                msg = "解析异常:" + ex.Message;
            }
            _logger.LogInformationWithProps(new { type = _logType, resourceId = resourceId }, "推送失败！！！" + resourceInfo + "\r\n" + msg);
            //_logServer.Info(_logType, "推送失败！！！" + resourceInfo + "\r\n" + msg, resourceId);
            return false;
        }

        /// <summary>
        /// 检查资源
        /// </summary>
        private bool CheckResource(string resourceID, string fromPath)
        {
            string xmlId = resourceID + "_xml";
            if (Utils.IsEmpty(resourceID) || _data.ServiceName != "测试环境")
                return false;

            if (RCFastDfs.Init.Exist(resourceID).Success && RCFastDfs.Init.Exist(xmlId).Success)
                return true;

            string tempPath = Utils.GetTempPath + Path.GetFileName(fromPath);
            string tempXmlPath = tempPath.Replace(Path.GetExtension(fromPath), ".xml");
            string fastDfsUrl = @"http://10.102.0.89:8080/api/file/download/";
            if (!File.Exists(tempPath))
            {
                DownloadFastDfs.RunDownload(tempPath, fastDfsUrl, resourceID);
                UploadFastDfs.UploadToFastDFSParser(tempPath, resourceID, resourceID);
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }

            if (!File.Exists(tempXmlPath))
            {
                DownloadFastDfs.RunDownload(tempXmlPath, fastDfsUrl, xmlId);
                UploadFastDfs.UploadToFastDFSParser(tempXmlPath, xmlId, resourceID);
                if (File.Exists(tempXmlPath))
                    File.Delete(tempXmlPath);
            }

            return true;
            //return RCFastDfs.Init.Exist(resourceID).Success && RCFastDfs.Init.Exist(xmlId).Success;
        }
        #endregion


        #region 消息消费
        /// <summary>
        /// 消费者执行入口
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="pdfMsg"></param>
        public void ConsumerExecute(string msg, out string pdfMsg)
        {

            if (Utils.IsEmpty(msg))
            {
                pdfMsg = "消息体为空.";
                DingDing.Send("无线表格解析异常通知：", pdfMsg, 0);
                UpdateNoLineRecord(_resData.ResID, IsPriority.AiApiFail);
                return;
            }

            pdfMsg = "";
            OcrPdf pdf = new OcrPdf();
            List<Page> jpgs = new List<Page>();

            try
            {
                string xmlFilePath = GetQueueXmlPath(msg, ref pdfMsg);
                if (Utils.IsEmpty(xmlFilePath))
                {
                    pdfMsg = "获取AI解析XML文件失败：" + pdfMsg;
                    bool isXmlFail = pdfMsg.Contains("html转XML异常");
                    IsPriority isPriority = isXmlFail ? IsPriority.ParserError : IsPriority.AiApiFail;
                    UpdateNoLineRecord(_resData?.ResID, isPriority, 0, 1, 0, _resData?.OutMd5);
                    //_logServer.Info(_logType, pdfMsg, _resData?.Info, _resData?.ResID);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, pdfMsg);
                    return;
                }

                ConsoleRegion.WriteToBuffer(false, "开始" + (_resData.XmlType.ToString() + ": ") + _resData.Info);

                ResultData<string> resultPdf = RCFastDfs.Init.DownLoad(_resData.ResID);
                NoLineToXmlHelper.Open(resultPdf.Result, out int pdfNum, _resData.ResID);

                #region 图片解析逻辑
                //根据已经解析的xml判断三大表页面

                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "图片解析逻辑开始：" + _resData?.Info);
                Dictionary<int, string> pageDic = GetFinancialPdf(xmlFilePath, out Dictionary<int, string> oldXml, pdfNum);
                string outDir = resultPdf.Result.ToLower().Replace(@".pdf", @"");
                string outPdfDir = resultPdf.Result.ToLower().Replace(@".pdf", @"");
                //无线解析不进行图片切分  暂时先将代码注释
                //if (pageDic.Count > 0 && !_resData.IsHK)
                //{
                //    //切分图片
                //    pdf = NoLineToXmlHelper.GetOcrPdf(false, out jpgs, false);
                //    if (jpgs == null || jpgs.Count == 0)
                //    {
                //        pdfMsg = "切成图片格式失败,解析失败";
                //        UpdateNoLineRecord(_resData.ResID, IsPriority.PdfDamage, pdfNum, 0, 1);
                //        DingDing.Send("无线表格异常通知：", pdfMsg, 0, _resData.Info);
                //        //_logServer.Info(_logType, pdfMsg + _resData.Info, _resData.ResID);
                //        return;
                //    }
                //    ////切分PDF
                //    //if (!SplitPdf(ref outPdfDir, ref pdf, ref pdfMsg, outDir, resultPdf.Result, _resInfo.Info, _resInfo.Title, pdfNum))
                //    //    return;
                //}

                //获取待解析的单页pdf
                GetImgPdfList(pageDic, outDir, outPdfDir, out Dictionary<int, string> textPdfDic, out Dictionary<int, string> ocrPdfDic);

                //OCR解析
                Dictionary<int, string> xmlImgDic = OcrImgParser(ocrPdfDic);

                string xmlContent = MergeXml(xmlImgDic, oldXml);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "图片解析逻辑结束：" + _resData?.Info);
                if (Utils.IsEmpty(xmlContent))
                {
                    pdfMsg = "文件解析失败，XML内容为空！";
                    DingDing.Send("无线表格解析异常通知：", pdfMsg, 0, _resData.ResID);
                    UpdateNoLineRecord(_resData.ResID, IsPriority.ParserError, pdfNum, 0, 1);
                    //_logServer.Info(_logType, pdfMsg + _resData.Info, _resData.ResID);
                    return;
                }
                #endregion               
                string temp_XmlPath = Utils.GetTempPath + _resData.ResID + "_noline.xml";
                File.WriteAllText(temp_XmlPath, xmlContent);
                //if (_resInfo.XmlType == 3)
                //{
                //    UploadFastDfs.ToFastDFS(temp_XmlPath, _resInfo.ResID + "_xml");
                //}

                //上传xml文件到fastdfs
                Result result = RCFastDfs.Init.Upload(temp_XmlPath, _resData.ResID + "_noline");

                if (_resData.XmlType.ToString().Contains("股票") && !RCFastDfs.Init.Exist(_resData.ResID + "_xml").Success)
                {
                    var tempResult = RCFastDfs.Init.Upload(temp_XmlPath, _resData.ResID + "_xml");
                    //_logServer.Info(_logType, "股票公告XML文件不存在，无线解析文件上传：" + _resData?.Info, _resData?.ResID);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "股票公告XML文件不存在，无线解析文件上传：" + _resData?.Info);

                }
                //else if (_resData.XmlType == NoLineType.文件特殊解析)
                //{
                //    var tempResult = RCFastDfs.Init.Upload(temp_XmlPath, _resData.ResID + "_xml");
                //    //_logServer.Info(_logType, "特殊文件解析，已覆盖上传XML文件：" + _resData?.Info, _resData?.ResID);
                //    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "特殊文件解析，已覆盖上传XML文件：" + _resData?.Info);
                //}

                if (!result.Success)
                {
                    _logger.LogErrorWithProps(new { type = _logType, resourceId = _resData?.ResID }, "无线表格解析XML上传失败" + "_resourceId:" + _resData.ResID);
                    //Utils.WriteLog("无线表格解析XML上传失败" + "_resourceId:" + _resData.ResID);
                    pdfMsg = "无线表格解析XML上传失败,解析失败";
                    DingDing.Send("无线表格解析异常通知：", "XML上传失败", 0, _resData.ResID);
                }

                //xml文件保存到共享目录
                WriteXMLFile(xmlContent);

                //删除临时文件
                DelTempFiles(pdfNum, outDir, outPdfDir, temp_XmlPath, xmlFilePath);

                //记录解析成功
                UpdateNoLineRecord(_resData.ResID, IsPriority.AutoDeal, pdfNum, 1, 0, _resData?.OutMd5);
            }
            catch (Exception ex)
            {
                _logger.LogErrorWithProps(new { type = _logType, resourceId = _resData?.ResID }, ex, msg + "，" + ex.Message);
                //Utils.WriteErrorLog(msg + "，" + ex.Message + ex.StackTrace);
                DingDing.Send("无线表格解析异常通知：NoLineToXmlQueue消费异常", ex.Message + "；" + msg, 0);
            }
            finally
            {
                //Dispose(frmPicture,pdf, _resData?.Info);                
                //_logServer.Info(_logType, "消费队列完成。" + _resData?.Info, _resData?.ResID);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "消费队列完成。" + _resData?.Info);
            }
        }

        private string GetQueueXmlPath(string msg, ref string pdfMsg)
        {
            //将返回的htmlToXml
            var resultsModel = JsonConvert.DeserializeObject<NoLineQueueModel>(msg);

            //获取资源信息
            GetResInfo(resultsModel.id);

            //_logServer.Info(_logType, "开始消费队列消息：" +ExtractJson.ToJson(resultsModel), resultsModel.id);
            _logger.LogInformationWithProps(new { type = _logType, resourceId = resultsModel.id }, "开始消费队列消息：" + ExtractJson.ToJson(resultsModel));
            if (resultsModel == null || resultsModel.code != "00" || !resultsModel.success
                || resultsModel.data == null || resultsModel.data.outPuts?.Count == 0)
            {
                pdfMsg = "解析消息体失败：" + resultsModel.message;
                //_logServer.Info(_logType, pdfMsg, resultsModel.id);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = resultsModel.id }, pdfMsg);

                if (resultsModel == null)
                    return "";

                string dingMsg = @"id：" + resultsModel.id + ",code：" + resultsModel.code + ",msg：" + resultsModel.message;
                resultsModel = null;
                string apiMsg = "";

                //队列解析失败调用补偿接口
                if (_resData != null)
                    resultsModel = FullTextToXml(ref apiMsg);

                if (resultsModel == null)
                {
                    dingMsg += Utils.IsEmpty(msg) ? "" : ",apiMsg:" + apiMsg;
                    DingDing.Send("无线表格解析异常通知：AI解析失败!", dingMsg, 0, _resData?.Info);
                    return "";
                }

            }

            //_logServer.Info(_logType, "获取公告信息结束：" + _resData.ToJson(), _resData.ResID);
            _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "获取公告信息结束：" + _resData.ToJson());
            //下载解析好的html
            foreach (var data in resultsModel.data.outPuts)
            {
                if (Utils.IsEmpty(data.outMd5))
                    continue;

                ResultData<string> resultXml = RCFastDfs.Init.DownLoad(data.outMd5);
                if (!resultXml.Success || Utils.IsEmpty(resultXml.Result))
                {
                    //_logServer.Info(_logType, "AI解析Xml文件下载失败：" + resultXml.Error, _resData.ResID);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "AI解析Xml文件下载失败：" + resultXml.Error);
                    continue;
                }

                _resData.OutMd5 = data.outMd5;
                string xmlContent = "";
                if (resultXml.Result.ToLower().EndsWith(".html"))
                {
                    xmlContent = NoLineToXmlFileParser.HtmlHeavy(resultXml.Result, "", -1, !_resData.IsHK);
                }
                else
                {
                    xmlContent = Utils.ReadTxtOrCsv(resultXml.Result);
                }
                if (!Utils.IsEmpty(resultXml.Result) && File.Exists(resultXml.Result)
                    && Utils.IsEmpty(xmlContent) || Regex.IsMatch(xmlContent, @"无内容|乱码"))
                {
                    var errStr = Utils.IsEmpty(xmlContent) ? "html转XML异常" : "读取html内容为" + (xmlContent == "无内容" ? "空" : "乱码");
                    pdfMsg = @"HTML文件解析正常，" + errStr;
                    DingDing.Send("无线表格解析异常通知：HtmlHeavy异常", pdfMsg, 0, _resData?.Info);
                    //_logServer.Info(_logType, "AI解析成功," + errStr + resultXml.Error, _resData.ResID);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "AI解析成功," + errStr + resultXml.Error);

                    if (xmlContent == "无内容")
                        xmlContent = "";

                    return xmlContent;
                }

                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "获取xmlPath成功; AI解析文件MD5:" + data.outMd5);
                //_logServer.Info(_logType, "获取xmlPath成功; AI解析文件MD5:" + data.outMd5, _resData.ResID);

                //全文解析返回 xml路径
                if (resultXml.Result.ToLower().EndsWith("html"))
                {
                    string xmlSavePath = resultXml.Result.Replace("html", "xml");
                    File.WriteAllText(xmlSavePath, xmlContent);
                    return xmlSavePath;
                }
                return resultXml.Result;
            }

            //_logServer.Info(_logType, "获取xmlPath结束." + _resData.Info, _resData.ResID);
            _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "获取xmlPath结束." + _resData.Info);

            return "";
        }

        //xml合并
        private string MergeXml(Dictionary<int, string> xmlTextDic, Dictionary<int, string> oldXmlDic)
        {
            Dictionary<int, string> xmlDic = new Dictionary<int, string>();
            foreach (var imgXml in oldXmlDic)
            {
                if (xmlTextDic.ContainsKey(imgXml.Key))
                    continue;

                xmlTextDic.Add(imgXml.Key, imgXml.Value);
            }
            string xmlContent = "";
            //排序
            foreach (var dic in xmlTextDic.OrderBy(a => a.Key))
            {
                xmlContent += dic.Value + "\r\n";
            }
            if (Utils.IsEmpty(xmlContent))
                return "";

            string content = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n" + "<div>\r\n" + xmlContent + "</div>";
            return content;
        }

        //图片OCR解析
        private Dictionary<int, string> OcrImgParser(Dictionary<int, string> textPdfDic)
        {
            Dictionary<int, string> xmlDic = new Dictionary<int, string>();
            OcrPdfService ocrService = new OcrPdfService();
            if (textPdfDic == null || textPdfDic.Count == 0)
                return new Dictionary<int, string>();

            foreach (var pdfPath in textPdfDic)
            {
                if (xmlDic.ContainsKey(pdfPath.Key))
                    continue;

                OcrPageModel ocrPageModel = ocrService.GetImgXml(new Page()
                {
                    ResourceID = _resData.ResID,
                    Index = pdfPath.Key,
                    Result = File.ReadAllBytes(pdfPath.Value),
                    FromPath = pdfPath.Value,
                });
                xmlDic.Add(pdfPath.Key, ocrPageModel.XmlContent);
            }
            return xmlDic;
        }

        //获取图片类型的pdf
        private static void GetImgPdfList(Dictionary<int, string> pageDic, string outDir, string outPdfDir, out Dictionary<int, string> textPdfList, out Dictionary<int, string> ocrPdfList)
        {
            textPdfList = new Dictionary<int, string>();
            ocrPdfList = new Dictionary<int, string>();
            if (pageDic == null || pageDic.Count == 0 || Utils.IsEmpty(outDir))
                return;

            foreach (var dic in pageDic)
            {
                string fileName = outDir + "_" + dic.Key + ".Jpeg";
                if (dic.Value != "img")
                    continue;

                ocrPdfList.Add(dic.Key, fileName);
            }
        }

        //获取待解析的pdf
        private static Dictionary<int, string> GetFinancialPdf(string xmlPath, out Dictionary<int, string> oldXml, int pdfNum)
        {
            Dictionary<int, string> pageDic = new Dictionary<int, string>();
            oldXml = new Dictionary<int, string>();
            XmlDocument doc = new XmlDocument();
            doc.Load(xmlPath);
            XmlNode root = doc.DocumentElement;
            var pages = root.SelectNodes("/div/page");
            if (pages == null || pages.Count == 0)
                return pageDic;

            List<XmlNode> pageNode = RemoveRepeatNode(pages, pdfNum);
            for (int i = 0; i < pageNode.Count; i++)
            {
                var page = pages[i];
                var pageType = page.Attributes["img"]?.Value == "false" ? "text" : "img";
                int num = Utils.IsEmpty(page.Attributes["num"]?.Value) ? -1 : Convert.ToInt32(page.Attributes["num"]?.Value);
                if (num > 0 && !oldXml.ContainsKey(num - 1))
                {
                    oldXml.Add((num - 1), page.OuterXml);
                }

                if (!Utils.IsContentMessyCode(page.InnerText))// && pageType != "img"
                    continue;

                pageDic.Add(i, pageType);
            }
            return pageDic;
        }

        private static List<XmlNode> RemoveRepeatNode(XmlNodeList nodeList, int pdfNum)
        {

            List<XmlNode> pageNode = new List<XmlNode>();
            for (int i = 0; i < nodeList.Count; i++)
            {
                if (pageNode.Count > 0 && pageNode.Last().InnerXml == nodeList[i].InnerXml)
                    continue;

                pageNode.Add(nodeList[i]);
            }
            return pageNode;
        }

        #endregion

        #region 补偿接口

        private NoLineQueueModel FullTextToXml(ref string msg)
        {
            try
            {
                //_logServer.Info(_logType, "队列消费失败，执行接口补偿。", _resData?.ResID);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData?.ResID }, "队列消费失败，执行接口补偿。");
                string url = _data.ServiceName == "生产环境" ? Utils._appsetting["AiNoline"]["url"].ToString()
                                                            : Utils._appsetting["AiNoline"]["url_test"].ToString();
                //url = @"http://10.106.0.53:8080/api/wireless-pdf-queue";
                DateTime startTime = DateTime.Now;
                string response = FullTextUrl(_resData.ResID, url, !_resData.IsHK);
                TimeSpan ts = DateTime.Now.Subtract(startTime);
                DisplayMsg(_resData.ResID + "，调用接口用时：" + ts.TotalSeconds + "秒");
                //_logServer.Info(_logType, "执行接口补偿用时：" + ts.TotalSeconds + "秒", _resData.ResID);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "执行接口补偿用时：" + ts.TotalSeconds + "秒");
                if (Utils.IsEmpty(response))
                {
                    msg = "执行接口补偿,接口未响应或超时";
                    //_logServer.Info(_logType, "执行接口补偿失败，接口未响应或超时", _resData.ResID);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "执行接口补偿失败，接口未响应或超时");
                    return null;
                }

                //将返回的htmlToXml
                var resultsModel = JsonConvert.DeserializeObject<NoLineQueueModel>(response);
                if (resultsModel == null || resultsModel.code != "00" || resultsModel.data.outPuts.Count == 0)
                {
                    msg = "执行接口补偿失败：" + response;
                    //_logServer.Info(_logType, "执行接口补偿失败:" + JsonConvert.DeserializeObject(response), _resData.ResID);
                    _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "执行接口补偿失败:" + JsonConvert.DeserializeObject(response));
                    return null;
                }
                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, "执行接口补偿成功" + JsonConvert.DeserializeObject(response));
                //_logServer.Info(_logType, "执行接口补偿成功" + JsonConvert.DeserializeObject(response), _resData.ResID);
                return resultsModel;
            }
            catch (Exception ex)
            {
                msg = "执行接口补偿异常";
                DingDing.Send("无线表格解析异常通知：AI队列解析失败，调用补偿接口异常", ex.Message + ex.StackTrace, 0, _resData.Info);
                //_logServer.Info(_logType, msg+ "\r\n"+ ex.Message, _resData.ResID);
                _logger.LogInformationWithProps(new { type = _logType, resourceId = _resData.ResID }, msg + "\r\n" + ex.Message);
            }

            return null;
        }

        private string FullTextUrl(string resourceId, string url, bool isHK)
        {

            var response = HttpUtil.Post(new HttpItems()
            {
                URL = url,
                Data = BuildDataBuffer(resourceId, isHK, _resData.XmlType),
                Accept = "application/json;charset=utf-8;",
                ContentType = "application/json",
                Method = "POST",
                Timeout = 10 * 60 * 1000,
            }).Document;
            return response;
        }

        private string BuildDataBuffer(string resID, bool isStock, NoLineType xmlType)
        {
            string parseTemplate = templateMap.TryGetValue(((int)xmlType).ToString(), out var t) ? t : "PDF_J";
            var dataBuffer = new
            {
                id = resID,
                businessId = (isStock ? "A" : "H") + "_FINANCIAL_REPORT_EXTRACT",
                //imageStrategy = isHK ? 1 : 0,
                //imageStrategy = 2,
                md5 = resID,
                parseTemplate = parseTemplate,
                // A股去除页眉页脚，港股不去除页眉页脚
                removePageHeader = isStock,
                removePageFooter = isStock,
                targetDocType = new List<string> { "xml" }.ToArray(),

            }.ToJson();

            return dataBuffer;
        }

        #endregion

        //写XML文件
        private void WriteXMLFile(string xmlContent)
        {
            string xmlSavePath = _resData.StorePath.ToLower().Replace(@"\pdf\", @"\xml\").Replace(".pdf", "") + "_noline.xml";

            if (Utils.WriteTxtFile(xmlSavePath, xmlContent, true))
                return;

            Task.Factory.StartNew(() =>
            {
                SharedFolder.CheckSharedFolder();
                if (Utils.WriteTxtFile(xmlSavePath, xmlContent, true))
                    return;

                Utils.WriteLog("无线表格解析XML文件写入到共享目录失败：" + xmlSavePath);
            });
        }

        //删除解析后的资源
        private void DelTempFiles(int pdfNum, string outDir, string outPdfDir, string xmlSavePath, string xmlFilePath)
        {
            for (int i = 0; i < pdfNum; i++)
            {
                Utils.DeleteFile(outDir + "_" + i + ".Jpeg");
                Utils.DeleteFile(outPdfDir + "_" + i + ".pdf");
            }

            //解析后的文件
            Utils.DeleteFile(xmlSavePath);
            //AI解析的html文件
            Utils.DeleteFile(xmlFilePath);
            //AI解析后转换的XML文件
            Utils.DeleteFile(xmlFilePath.Replace("xml", "html"));

            if (Utils.IsEmpty(outDir))
                return;

            Utils.DeleteFile(outDir.Replace("_new", "") + ".pdf");
            Utils.DeleteFile(outDir + "_xml.xml");
        }

        private void GetResInfo(string resID)
        {
            string sql = @"	SELECT a.Title,a.storePath,b.moduleID,c.noLineXmlType FROM cfg.dmip_Resource(NOLOCK) a
                            INNER JOIN cfg.dmip_ResourceStatus(NOLOCK) b ON a.ob_object_id =b.ob_object_id
	                        INNER JOIN cfg.dmip_ArchiveRule(NOLOCK) c ON b.moduleID =c.moduleID
	                        WHERE a.ob_object_id ='" + resID + "'";
            DataTable dt = _data.GetDataTable(sql);
            if (dt == null || dt.Rows.Count != 1)
            {
                sql = @" SELECT a.Title,a.storePath,b.moduleID,c.noLineXmlType FROM cfg.dmip_Resource_bak(NOLOCK) a
                         INNER JOIN cfg.dmip_ResourceStatus(NOLOCK) b ON a.ob_object_id =b.ob_object_id
	                     INNER JOIN cfg.dmip_ArchiveRule(NOLOCK) c ON b.moduleID =c.moduleID
	                     WHERE a.ob_object_id ='" + resID + "'";
                dt = _data.GetDataTable(sql);
                if (dt == null || dt.Rows.Count != 1)
                    return;
            }

            int.TryParse(dt.Rows[0]["noLineXmlType"]?.ToString(), out int noLineXmlType);

            _resData = new ResData()
            {
                ResID = resID,
                ModuleID = dt.Rows[0]["moduleID"].ToString(),
                Title = dt.Rows[0]["title"].ToString(),
                StorePath = dt.Rows[0]["storePath"].ToString(),
                XmlType = (NoLineType)noLineXmlType,
            };
        }

        public static bool IsNoLineRecord(string resourceId, int priority = -1)
        {
            if (priority == (int)IsPriority.ResetRes)
                return false;

            string sql = @"select ob_object_id from cfg.dmip_ResourceStatus(nolock) where ob_object_id='" + resourceId + "'";
            DataTable dt = _data.GetDataTable(sql, GlobalData.CommonMenuID);
            if (Utils.IsEmpty(dt))
                return false;

            return true;
        }

        public static bool InsertNoLineRecord(string resourceId, string moduleId, DataRow rd, int xmlType = -1, int priority = -1)
        {
            if (priority == (int)IsPriority.ResetRes)
                return true;

            DataTable table = _data.GetStructure("cfg.dmip_ResourceStatus", GlobalData.CommonMenuID);
            if (table == null || !table.Columns.Contains("ob_object_id"))
                return false;

            var row = table.NewRow();
            row["ob_object_id"] = resourceId;
            row["isPriority"] = (int)IsPriority.InProcess;
            row["igsdm"] = rd["igsdm"];
            row["ggrq"] = rd["ggrq"];
            row["moduleID"] = moduleId;
            row["isOpen"] = 1;
            row["resMD5"] = rd["resMD5"];
            row["createTime"] = DateTime.Now;
            row["xmlType"] = xmlType;
            table.Rows.Add(row);
            bool success = _data.DataImport("cfg.dmip_ResourceStatus", table, ModifyType.Insert, GlobalData.CommonMenuID);
            return success;
        }

        public bool UpdateNoLineRecord(string resourceId, IsPriority isPriority, int pageCount = 0, int isOpen = 1, int dingding = 0, string htmlID = "")
        {
            string sql = @"select * from cfg.dmip_ResourceStatus(nolock) where ob_object_id ='" + resourceId + "'";
            DataTable table = _data.GetDataTable(sql, GlobalData.CommonMenuID);
            if (table == null || table.Rows.Count != 1)
                return false;

            if (isPriority == IsPriority.ResetRes)
            {
                table.Rows[0]["createTime"] = DateTime.Now;
            }
            else
            {
                table.Rows[0]["isPriority"] = (int)isPriority;
                table.Rows[0]["isOpen"] = isOpen;
                table.Rows[0]["status"] = 0;
                table.Rows[0]["dingding"] = dingding;
                table.Rows[0]["htmlID"] = Utils.IsEmpty(htmlID) ? table.Rows[0]["htmlID"] : htmlID;

                if (pageCount != 0)
                    table.Rows[0]["pageCount"] = pageCount;
                if (isPriority == IsPriority.AutoDeal)
                    table.Rows[0]["endTime"] = DateTime.Now;
            }

            bool success = _data.DataImport("cfg.dmip_ResourceStatus", table, ModifyType.Update, GlobalData.CommonMenuID);
            if (success && isPriority == IsPriority.AutoDeal && isSendTask)
            {
                SendDirectToTaskMQ("('" + resourceId + "')", table.Rows[0]["moduleID"].ToString());
            }
            return success;
        }
        /// <summary>
        /// 发送消息到任务队列(无线表格解析服务)
        /// </summary>
        /// <param name="resourceIDs">资源ID集合</param>
        /// <param name="categoryID">分类ID</param>
        /// <param name="isResetTask">是否重置任务</param>
        /// <returns>是否发送成功</returns>
        public bool SendDirectToTaskMQ(string resourceIDs, string categoryID, bool isResetTask = false)
        {
            var routingConfigs = GetRoutingConfigurations();
            if (!routingConfigs.Any())
                return false;
            var resources = DeleteWebMessage.GetAutoResource(resourceIDs);
            if (resources == null || !resources.Any())
            {
                return false;
            }

            foreach (var resource in resources)
            {
                var categoryIds = GetCategoryIds(categoryID, routingConfigs, resource, isResetTask);
                foreach (var catId in categoryIds)
                {
                    if (!isResetTask && IsAlreadyArchived(catId, resource.ObjectID))
                        continue;
                    PrepareResourceForSending(resource, catId, isResetTask);
                    if (!routingConfigs.ContainsKey(catId))
                        continue;
                    SendMessage(resource, routingConfigs[catId]);
                }
            }

            return true;
        }

        /// <summary>
        /// 获取路由配置信息
        /// </summary>
        /// <returns>路由配置信息字典</returns>
        private Dictionary<RedisValue, Tuple<string, string>> GetRoutingConfigurations()
        {
            var hashFields = _commonRedisCache.RedisDatabase.HashGetAll("DMP:Auto:specialTaskRoutings");
            return hashFields.ToDictionary(
                hashField => hashField.Key,
                hashField =>
                {
                    var routings = hashField.Value.ToString().Split(',');
                    var routingKey = routings[0];
                    var exchange = routings.Length > 1 ? routings[1] : "AutoTask";
                    return new Tuple<string, string>(routingKey, exchange);
                });
        }

        /// <summary>
        /// 获取需要发送的分类ID集合
        /// </summary>
        /// <param name="categoryID">分类ID</param>
        /// <param name="routingConfigs">路由配置信息</param>
        /// <param name="resource">资源对象</param>
        /// <param name="isResetTask">是否重置任务</param>
        /// <returns>分类ID集合</returns>
        private List<string> GetCategoryIds(string categoryID, Dictionary<RedisValue, Tuple<string, string>> routingConfigs, AutoResource resource, bool isResetTask)
        {
            var categoryIds = new List<string> { categoryID };
            var routingKey = "noLineDirectPush";
            if (routingConfigs.ContainsKey(categoryID))
            {
                routingKey = routingConfigs[categoryID].Item1;
            }
            if (!isResetTask)
            {
                var moduleIdList = routingConfigs.Where(kvp => kvp.Value.Item1 == routingKey).Select(kvp => kvp.Key.ToString()).ToList();
                categoryIds = DeleteWebMessage.GetResCategoryIDList(resource.ObjectID).Intersect(moduleIdList).ToList();
            }
            return categoryIds;
        }

        /// <summary>
        /// 检查资源是否已经归档
        /// </summary>
        /// <param name="categoryId">分类ID</param>
        /// <param name="resourceId">资源ID</param>
        /// <returns>是否已经归档</returns>
        private bool IsAlreadyArchived(string categoryId, string resourceId)
        {
            string query = $"SELECT * FROM cfg.dmip_ArchiveData(nolock) WHERE moduleID = '{categoryId}' AND resourceID = '{resourceId}'";
            DataTable dtArchiveData = _data.GetDataTable(query, GlobalData.CommonMenuID);
            return !Utils.IsEmpty(dtArchiveData) && dtArchiveData.Rows[0]["finishStatus"]?.ToString() == "1";
        }

        /// <summary>
        /// 准备资源对象以便发送
        /// </summary>
        /// <param name="resource">资源对象</param>
        /// <param name="categoryId">分类ID</param>
        /// <param name="isResetTask">是否重置任务</param>
        private void PrepareResourceForSending(AutoResource resource, string categoryId, bool isResetTask)
        {
            resource.CategoryID = categoryId;
            resource.IsResetTask = isResetTask;
            resource.Priority = 1;
            resource.ArchiveInfos = new List<ArchiveInfo> { new ArchiveInfo { ModuleID = categoryId, Priority = 1 } };
        }

        /// <summary>
        /// 发送消息到消息队列
        /// </summary>
        /// <param name="resource">资源对象</param>
        /// <param name="routingConfig">路由配置信息</param>
        private void SendMessage(AutoResource resource, Tuple<string, string> routingConfig)
        {
            string msg = ExtractJson.ToJson(resource);
            SendMessageMQ.SendMessage(_dmpChannel, routingConfig.Item2, "direct", routingConfig.Item1, msg, Convert.ToByte(resource.Priority));
            Utils.WriteLog($"SendDirectToTaskMQ: {resource.ObjectID} 推送到指定队列成功（exchange: {routingConfig.Item2}, routingKey: {routingConfig.Item1}）");
        }

        public static Tuple<bool, string> ResetResource(List<DataRow> selectedRows, string cateID)
        {
            if (selectedRows == null || selectedRows.Count == 0 || Utils.IsEmpty(cateID))
                return new Tuple<bool, string>(false, @"未选择需要重置的资源！！！");

            string resourceIDs = "(";
            foreach (DataRow dr in selectedRows)
            {
                string resourceID = dr["ob_object_id"].ToString();
                resourceIDs += "'" + resourceID + "',";

                if (!dr.Table.Columns.Contains("title"))
                    continue;

                string tempPath = Utils.GetTempPath + Utils.ClearFileNameSpecificChar(dr["title"].ToString()) + "_noline.xml";
                if (!File.Exists(tempPath))
                    continue;

                File.Delete(tempPath);

            }
            resourceIDs = resourceIDs.Substring(0, resourceIDs.Length - 1) + ")";
            string q = @"select * from cfg.dmip_ResourceStatus(nolock) where isPriority <> 6 and ob_object_id in " + resourceIDs;
            DataTable dt = _data.GetDataTable(q, GlobalData.CommonMenuID);
            if (Utils.IsEmpty(dt))
                return new Tuple<bool, string>(false, @"未查询到解析数据！！！");

            foreach (DataRow dr in dt.Rows)
            {
                dr["status"] = 6;
                dr["isPriority"] = 6;
                dr["createTime"] = DateTime.Now;
            }
            bool success = _data.DataImport("cfg.dmip_ResourceStatus", dt, ModifyType.Update, GlobalData.CommonMenuID);
            if (success)
                return new Tuple<bool, string>(true, @"无线解析重置成功！！！");

            return new Tuple<bool, string>(false, _data.GetLastMessage(GlobalData.CommonMenuID));
        }

        public void DisplayMsg(string msg)
        {
            ConsoleRegion.WriteToBuffer(true, msg);
        }

        public void DisplayMsg2(string msg)
        {
            ConsoleRegion.WriteToBuffer(false, msg);
        }

    }

    public class ResData
    {
        /// <summary>
        /// 资源ID
        /// </summary>
        public string ResID { get; set; }
        /// <summary>
        /// 节点ID
        /// </summary>
        public string ModuleID { get; set; }
        /// <summary>
        /// 文件路劲
        /// </summary>
        public string StorePath { get; set; }
        /// <summary>
        /// 标题
        /// </summary>
        /// </summary>
        public string Title { get; set; }
        /// <summary>
        /// 资源ID+标题
        /// </summary>
        public string Info { get => ResID + "_" + Title; }

        public NoLineType XmlType { get; set; }

        public bool IsHK { get => XmlType.ToString().Contains("港股"); }

        public string OutMd5 { get; set; }
    }
}
