# http2-flow-control-simulator

模拟 HTTP/2 流窗口、背压和设置变更的流控引擎（Python 3.8.2+）。

## 定位

这是一个**流控状态模拟器**：它按事件逐步维护发送端的连接窗口与各流窗口，
计算"当前可发送字节"，用于复现代理层偶发的 HTTP/2 请求卡死问题（流一直等待
WINDOW_UPDATE、连接窗口与流窗口额度不一致、SETTINGS 变更后出现负的有效窗口）。

它**不负责帧编解码**：不解析/生成任何线上字节流，输入是抽象事件，输出是
每个事件后的状态快照。

## 事件模型

`h2_flow_control.Event` 支持以下事件类型：

| 事件 | 说明 |
| --- | --- |
| `HEADERS` | 打开一个流（stream id 必须递增且未使用过） |
| `DATA` | 消耗连接窗口和对应流窗口各 `size` 字节 |
| `WINDOW_UPDATE` | 给连接（`stream_id=0`）或某个流增加 `increment` 额度 |
| `SETTINGS` | 修改 `SETTINGS_INITIAL_WINDOW_SIZE`，delta 作用于所有未关闭的流 |
| `END_STREAM` | 本端停止发送（半关闭），不再计入阻塞流 |
| `RST_STREAM` | 立即关闭流，释放其占用的连接额度 |

遵循 RFC 7540 的关键语义：

- SETTINGS 收缩可以把流窗口压成**负数**，必须用后续 WINDOW_UPDATE 偿还后
  才能继续发送（6.9.2 节）。
- WINDOW_UPDATE 增量为 0：流级是 stream error，连接级是 connection error。
- 窗口超过 2^31-1：流窗口溢出只终止该流，连接窗口溢出终止整个连接。
- 关闭的流不再受 SETTINGS 影响，也不出现在阻塞列表中。
- 乱序输入（未打开就发 DATA、stream id 回退、操作已关闭流）会被检测为
  协议错误。

## 错误隔离

- **stream error**：只重置受影响的流，连接和其他流继续工作。
- **connection error**：整个连接停止，后续事件被拒绝且不再改变任何状态，
  避免错误污染其他流的记账。

## 使用

```python
from h2_flow_control import (
    FlowControlSimulator, Event, HEADERS, DATA, WINDOW_UPDATE, SETTINGS,
)

sim = FlowControlSimulator()
for step in sim.run([
    Event(HEADERS, stream_id=1),
    Event(DATA, stream_id=1, size=65535),
    Event(SETTINGS, initial_window_size=0),      # 流窗口变为 -65535
    Event(WINDOW_UPDATE, stream_id=1, increment=70000),
]):
    print(step.connection_window, step.stream_windows,
          step.blocked_streams, step.error)
```

直接运行 `python h2_flow_control.py` 可以看到一段演示轨迹。

## 测试

```sh
python -m unittest
```

测试覆盖：窗口收缩、负窗口恢复、超大/零增量、流重置、交错更新、
输入事件乱序检测。
