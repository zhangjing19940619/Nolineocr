using System;
using System.Collections.Generic;
using System.Threading;

public static class ConsoleRegion
{
    // 配置参数
    private const int MaxBufferLines = 500;
    private const int VisibleLines = 50;
    private static readonly object _lock = new object();
    private static List<string> _top = new List<string>();
    private static List<string> _bottom = new List<string>();

    /// <summary>
    /// 写入内容到指定区域
    /// </summary>
    /// <param name="isTop">true=上方区域，false=下方区域</param>
    /// <param name="content">要显示的内容</param>
    public static void WriteToBuffer(bool isTop, string content)
    {
        lock (_lock)
        {
            var buffer = isTop ? _top : _bottom;

            // 添加带时间戳的内容
            foreach (var line in content.Split('\n'))
            {
                var timestamp = DateTime.Now.ToString("[yyyy-MM-dd HH:mm:ss.fff] ");
                buffer.Add($"{timestamp}{line.Trim()}");
            }

            // 自动清理旧数据
            if (buffer.Count > MaxBufferLines)
                buffer.RemoveRange(0, buffer.Count - VisibleLines);

            Redraw();
        }
    }

    /// <summary>
    /// 刷新控制台显示
    /// </summary>
    private static void Redraw()
    {
        try
        {
            Console.Clear();
            // 绘制上方区域
            var startIndex = Math.Max(0, _top.Count - VisibleLines);
            for (int i = 0; i < Math.Min(VisibleLines, _top.Count); i++)
            {
                Console.WriteLine(_top[startIndex + i]);
            }

            // 绘制分隔线
            Console.WriteLine(new string('═', Console.WindowWidth - 1));

            // 绘制下方区域
            startIndex = Math.Max(0, _bottom.Count - VisibleLines);
            for (int i = 0; i < Math.Min(VisibleLines, _bottom.Count); i++)
            {
                Console.WriteLine(_bottom[startIndex + i]);
            }
        }
        catch (Exception)
        {           
        }

      
    }
}
