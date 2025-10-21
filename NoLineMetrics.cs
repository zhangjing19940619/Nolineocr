using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Deduce.DMIP.NoLineOcrParse
{
    internal class NoLineMetrics
    {
        internal static Gauge CurrentTasks = Metrics
           .CreateGauge("current_tasks", $"Current running background task.");

        /// <summary>
        /// 表达式计算性能统计, 
        /// </summary>
        internal static Histogram ExpressionCalcTime = Metrics
            .CreateHistogram("expression_calculation_time", "MilliSeconds of Expression Calculation Time of Resource"
            , new HistogramConfiguration
            {
                Buckets = Histogram.ExponentialBuckets(2, 2, 16),
            });
    }
}
