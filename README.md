# useful_command

## JSONL 动态看板

一个本地运行的通用看板，使用 Python 标准库和原生 HTML/JavaScript，无需安装第三方依赖。
需要 Python 3.9+ 和现代浏览器。

```bash
cd /Users/ben/git_projects/useful_command
python3 jsonl_dashboard.py jsonl_dashboard/example.jsonl
```

在浏览器打开 <http://127.0.0.1:8765>。
监听自己的文件时，替换命令中的文件路径即可，也可以直接监听 `request_llm.py` 生成的输出文件：

```bash
python3 jsonl_dashboard.py /absolute/path/results.jsonl --port 8765 --max-records 10000
```

- 默认每 2 秒刷新，可切换间隔、暂停或立即刷新。
- 自动识别数值字段，显示均值、最新值和最近 120 个数值的趋势；横轴是记录序号。
- 自动识别字符串和布尔字段，显示计数最多的 8 类；默认优先选择 `status`。
- 嵌套对象使用点分路径，例如 `metrics.score`。数组和完整对象可以在记录详情中查看。
- 支持所有字段的文本搜索、每页 25 条的分页和点击记录查看完整 JSON。表格展示当前页前 8 个字段。
- 总数统计当前文件中已读取的所有有效记录；图表、搜索、均值和分页统计最近 `--max-records` 条。

### 输入与实时写入

每行必须是一个 JSON 对象，字段可以不同，不需要预先定义 schema：

```json
{"id":1,"status":"success","latency_ms":820,"metrics":{"score":0.92}}
{"id":2,"status":"error","latency_ms":1500,"metrics":{"score":0.35}}
```

写入程序需要在每条记录后添加换行并刷新缓冲区：

```python
import json

with open("results.jsonl", "a", encoding="utf-8") as output:
    output.write(json.dumps({"id": 3, "status": "success", "latency_ms": 610}, ensure_ascii=False) + "\n")
    output.flush()
```

可以先启动看板，再创建输入文件。文件末尾没有换行的内容暂不计入，等待写入完成。
空行忽略；非法 JSON、非对象记录、非有限浮点数和超过 1 MiB 的单行会跳过并计数。
输入文件以 UTF-8 编码。

### 实现方式与边界

`jsonl_dashboard.py` 提供本地 HTTP 服务和 `/api/snapshot` 接口；浏览器轮询接口，
后端记录字节偏移，每次只读取新增内容，并用有长度上限的队列保存最近记录。
每次请求最多读取 4 MiB，大文件会分多次刷新加载。识别到文件截断、替换或删除后会重置统计。
该工具面向追加写入；任意修改文件中间内容不保证被检测到，需要重启服务重新读取。

均值和分类基于当前搜索结果；缺失字段不参与对应指标的统计。布尔值不当作数值。
选择高基数字段（例如 response 文本）时，分类图可能不适合观察分布，可切换到其他字段。
内存占用取决于保留条数和单条记录大小，可用 `--max-records` 调低。
默认只监听 `127.0.0.1`；`--host` 可修改地址，服务本身没有登录验证。
按 `Ctrl+C` 停止。

验证文件读取与 HTTP 接口：

```bash
python3 -m unittest -v test_jsonl_dashboard.py
```
