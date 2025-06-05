# DTraderHQ Python WebSocket 客户端

这是一个用于连接 DTraderHQ WebSocket API 的 Python 异步客户端库。

## 功能特性

- 异步 WebSocket 连接
- JWT 认证支持
- 股票数据订阅/取消订阅
- 批量订阅操作
- 重置订阅功能
- 自动心跳保持连接
- 完整的错误处理
- 事件回调机制

## 安装依赖

```bash
pip install -r requirements.txt
```

## 快速开始

### 基本使用

```python
import asyncio
from client import DTraderHQClient, MarketData

async def on_data(data: MarketData):
    print(f"收到数据: {data.stock_code} - 类型: {data.data_type}")

async def main():
    client = DTraderHQClient("ws://localhost:8080/ws")
    client.on_data = on_data
    
    await client.connect()
    await client.authenticate("your_jwt_token")
    await client.subscribe("000001", [4, 8])  # 订阅逐笔成交和逐笔大单
    
    # 保持连接
    await asyncio.sleep(60)
    
    await client.close()

asyncio.run(main())
```

## API 参考

### DTraderHQClient

#### 构造函数

```python
client = DTraderHQClient(url, ping_interval=30)
```

- `url`: WebSocket 服务器地址
- `ping_interval`: 心跳间隔（秒），默认 30 秒

#### 主要方法

##### 连接和认证

```python
# 连接到服务器
await client.connect()

# 用户认证
await client.authenticate(token)

# 关闭连接
await client.close()
```

##### 订阅管理

```python
# 单个订阅
await client.subscribe(stock_code, data_types)

# 批量订阅
await client.batch_subscribe(subscriptions)

# 取消订阅
await client.unsubscribe(stock_code)

# 批量取消订阅
await client.batch_unsubscribe(stock_codes)

# 重置订阅
await client.reset_subscriptions(subscriptions)

# 获取当前订阅
subscriptions = client.get_subscriptions()
```

##### 状态检查

```python
# 检查连接状态
if client.is_connected:
    print("已连接")

# 检查认证状态
if client.is_authenticated:
    print("已认证")
```

#### 事件回调

```python
# 设置数据回调
client.on_data = async def(data: MarketData):
    # 处理市场数据
    pass

# 设置错误回调
client.on_error = async def(error: str):
    # 处理错误
    pass

# 设置成功回调
client.on_success = async def(msg_type: str, data: dict):
    # 处理成功消息
    pass

# 设置连接回调
client.on_connected = async def():
    # 连接成功
    pass

# 设置认证回调
client.on_authenticated = async def():
    # 认证成功
    pass
```

### MarketData

市场数据对象包含以下字段：

```python
class MarketData:
    stock_code: str      # 股票代码
    data_type: int       # 数据类型
    timestamp: int       # 时间戳
    data: dict          # 具体数据内容
```

### 数据类型

- `4`: 逐笔成交数据
- `8`: 逐笔大单数据
- `14`: 逐笔委托数据

## 示例代码

### 基本示例

查看 `examples/basic_example.py` 了解完整的使用流程。


## 错误处理

客户端会自动处理以下错误：

- 连接断开自动重连
- 认证失败
- 订阅错误
- 网络异常

通过设置 `on_error` 回调函数可以接收错误通知。

## 注意事项

1. 确保在使用前先连接和认证
2. 使用完毕后记得调用 `close()` 方法清理资源
3. 所有操作都是异步的，需要使用 `await` 关键字
4. 建议在生产环境中添加适当的错误处理和重连逻辑

## 许可证

本项目遵循 MIT 许可证。