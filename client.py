import asyncio
import json
import logging
import time
from typing import Dict, List, Any, Optional, Callable, Union, Set

import websockets

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('DTraderHQClient')

# 消息类型常量
MESSAGE_TYPE_AUTH = "auth"
MESSAGE_TYPE_SUBSCRIBE = "subscribe"
MESSAGE_TYPE_UNSUBSCRIBE = "unsubscribe"
MESSAGE_TYPE_BATCH_SUBSCRIBE = "batch_subscribe"
MESSAGE_TYPE_BATCH_UNSUBSCRIBE = "batch_unsubscribe"
MESSAGE_TYPE_RESET = "reset"
MESSAGE_TYPE_DATA = "data"
MESSAGE_TYPE_ERROR = "error"
MESSAGE_TYPE_SUCCESS = "success"
MESSAGE_TYPE_PING = "ping"
MESSAGE_TYPE_PONG = "pong"


class MarketData:
    """市场数据类"""
    
    def __init__(self, stock_code: str, data_type: int, data: Any, timestamp: int):
        self.stock_code = stock_code
        self.data_type = data_type
        self.data = data
        self.timestamp = timestamp

    def __str__(self) -> str:
        return f"MarketData(stock_code={self.stock_code}, data_type={self.data_type}, timestamp={self.timestamp})"


class DTraderHQClient:
    """DTraderHQ WebSocket客户端"""
    
    def __init__(self, url: str, ping_interval: float = 30.0, timeout: float = 10.0):
        """
        初始化客户端
        
        Args:
            url: WebSocket服务器URL
            ping_interval: 心跳间隔（秒）
            timeout: 操作超时时间（秒）
        """
        self.url = url
        self.ping_interval = ping_interval
        self.timeout = timeout
        
        self.ws = None
        self.is_connected = False
        self.is_authenticated = False
        self.subscriptions: Dict[str, List[int]] = {}  # stockCode -> dataTypes
        
        # 任务和锁
        self.listen_task = None
        self.ping_task = None
        self.lock = asyncio.Lock()
        
        # 回调函数
        self.on_connected: Optional[Callable[[], None]] = None
        self.on_disconnected: Optional[Callable[[], None]] = None
        self.on_authenticated: Optional[Callable[[], None]] = None
        self.on_data: Optional[Callable[[MarketData], None]] = None
        self.on_error: Optional[Callable[[str], None]] = None
        self.on_success: Optional[Callable[[str, Any], None]] = None
    
    async def connect(self) -> None:
        """连接到WebSocket服务器"""
        if self.is_connected:
            logger.warning("已经连接到服务器")
            return
        
        try:
            self.ws = await websockets.connect(self.url)
            self.is_connected = True
            logger.info(f"已连接到服务器: {self.url}")
            
            # 启动监听和心跳任务
            self.listen_task = asyncio.create_task(self._listen_loop())
            self.ping_task = asyncio.create_task(self._ping_loop())
            
            if self.on_connected:
                await self.on_connected()
        except Exception as e:
            logger.error(f"连接失败: {e}")
            raise
    
    async def close(self) -> None:
        """关闭连接"""
        if not self.is_connected:
            return
        
        self.is_connected = False
        self.is_authenticated = False
        
        # 取消任务
        if self.listen_task:
            self.listen_task.cancel()
        if self.ping_task:
            self.ping_task.cancel()
        
        # 关闭WebSocket连接
        if self.ws:
            await self.ws.close()
            self.ws = None
        
        logger.info("已关闭连接")
        if self.on_disconnected:
            await self.on_disconnected()
    
    async def authenticate(self, token: str) -> None:
        """进行认证"""
        if not self.is_connected:
            raise RuntimeError("未连接到服务器")
        
        auth_msg = {
            "type": MESSAGE_TYPE_AUTH,
            "data": {"token": token},
            "timestamp": int(time.time() * 1000)
        }
        
        await self.send_message(auth_msg)
        logger.info("已发送认证请求")
    
    async def subscribe(self, stock_code: str, data_types: List[int]) -> None:
        """订阅股票数据"""
        if not self.is_authenticated:
            raise RuntimeError("未认证")
        
        sub_msg = {
            "type": MESSAGE_TYPE_SUBSCRIBE,
            "data": {
                "stock_code": stock_code,
                "data_types": data_types
            },
            "timestamp": int(time.time() * 1000)
        }
        
        # 更新本地订阅记录
        async with self.lock:
            self.subscriptions[stock_code] = data_types
        
        await self.send_message(sub_msg)
        logger.info(f"已发送订阅请求: {stock_code}, 数据类型: {data_types}")
    
    async def unsubscribe(self, stock_code: str) -> None:
        """取消订阅"""
        if not self.is_authenticated:
            raise RuntimeError("未认证")
        
        unsub_msg = {
            "type": MESSAGE_TYPE_UNSUBSCRIBE,
            "data": {"stock_code": stock_code},
            "timestamp": int(time.time() * 1000)
        }
        
        # 更新本地订阅记录
        async with self.lock:
            if stock_code in self.subscriptions:
                del self.subscriptions[stock_code]
        
        await self.send_message(unsub_msg)
        logger.info(f"已发送取消订阅请求: {stock_code}")
    
    async def batch_subscribe(self, subscriptions: List[Dict[str, Union[str, List[int]]]]) -> None:
        """批量订阅股票数据"""
        if not self.is_authenticated:
            raise RuntimeError("未认证")
        
        if not subscriptions:
            raise ValueError("订阅列表为空")
        
        if len(subscriptions) > 100:
            raise ValueError("订阅数量超过限制（最大100）")
        
        batch_msg = {
            "type": MESSAGE_TYPE_BATCH_SUBSCRIBE,
            "data": {"subscriptions": subscriptions},
            "timestamp": int(time.time() * 1000)
        }
        
        # 更新本地订阅记录
        async with self.lock:
            for sub in subscriptions:
                stock_code = sub["stock_code"]
                data_types = sub["data_types"]
                self.subscriptions[stock_code] = data_types
        
        await self.send_message(batch_msg)
        logger.info(f"已发送批量订阅请求: {len(subscriptions)} 只股票")
    
    async def batch_unsubscribe(self, stock_codes: List[str]) -> None:
        """批量取消订阅"""
        if not self.is_authenticated:
            raise RuntimeError("未认证")
        
        if not stock_codes:
            raise ValueError("股票代码列表为空")
        
        if len(stock_codes) > 100:
            raise ValueError("股票代码数量超过限制（最大100）")
        
        batch_msg = {
            "type": MESSAGE_TYPE_BATCH_UNSUBSCRIBE,
            "data": {"stock_codes": stock_codes},
            "timestamp": int(time.time() * 1000)
        }
        
        # 更新本地订阅记录
        async with self.lock:
            for stock_code in stock_codes:
                if stock_code in self.subscriptions:
                    del self.subscriptions[stock_code]
        
        await self.send_message(batch_msg)
        logger.info(f"已发送批量取消订阅请求: {len(stock_codes)} 只股票")
    
    async def reset_subscriptions(self, subscriptions: List[Dict[str, Union[str, List[int]]]]) -> None:
        """重置订阅（取消所有当前订阅并设置新的订阅）"""
        if not self.is_authenticated:
            raise RuntimeError("未认证")
        
        if len(subscriptions) > 100:
            raise ValueError("订阅数量超过限制（最大100）")
        
        reset_msg = {
            "type": MESSAGE_TYPE_RESET,
            "data": {"subscriptions": subscriptions},
            "timestamp": int(time.time() * 1000)
        }
        
        # 重置本地订阅记录
        async with self.lock:
            self.subscriptions.clear()
            for sub in subscriptions:
                stock_code = sub["stock_code"]
                data_types = sub["data_types"]
                self.subscriptions[stock_code] = data_types
        
        await self.send_message(reset_msg)
        logger.info(f"已发送重置订阅请求: {len(subscriptions)} 只股票")
    
    async def send_message(self, message: Dict[str, Any]) -> None:
        """发送消息"""
        if not self.is_connected or not self.ws:
            raise RuntimeError("未连接到服务器")
        
        try:
            await self.ws.send(json.dumps(message))
        except Exception as e:
            logger.error(f"发送消息失败: {e}")
            raise
    
    def get_subscriptions(self) -> Dict[str, List[int]]:
        """获取当前订阅列表"""
        return self.subscriptions.copy()
    
    async def _listen_loop(self) -> None:
        """监听消息循环"""
        if not self.ws:
            return
        
        try:
            while self.is_connected:
                try:
                    message = await self.ws.recv()
                    await self._handle_message(message)
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("连接已关闭")
                    break
                except Exception as e:
                    logger.error(f"接收消息错误: {e}")
        finally:
            # 确保连接关闭时更新状态
            if self.is_connected:
                self.is_connected = False
                self.is_authenticated = False
                if self.on_disconnected:
                    await self.on_disconnected()
    
    async def _ping_loop(self) -> None:
        """心跳循环"""
        try:
            while self.is_connected:
                await asyncio.sleep(self.ping_interval)
                
                if not self.is_connected:
                    break
                
                ping_msg = {
                    "type": MESSAGE_TYPE_PING,
                    "timestamp": int(time.time() * 1000)
                }
                
                try:
                    await self.send_message(ping_msg)
                    logger.debug("已发送心跳")
                except Exception as e:
                    logger.error(f"发送心跳失败: {e}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"心跳循环错误: {e}")
    
    async def _handle_message(self, message_str: str) -> None:
        """处理接收到的消息"""
        try:
            message = json.loads(message_str)
            msg_type = message.get("type")
            
            if msg_type == MESSAGE_TYPE_SUCCESS:
                # 处理成功消息，包括认证成功
                data = message.get("data", {})
                if isinstance(data, dict) and data.get("message") == "认证成功":
                    self.is_authenticated = True
                    logger.info("认证成功")
                    if self.on_authenticated:
                        await self.on_authenticated()
                
                if self.on_success:
                    await self.on_success(msg_type, data)
            
            elif msg_type == MESSAGE_TYPE_AUTH:
                # 处理认证响应
                data = message.get("data", {})
                if isinstance(data, dict) and data.get("message") == "认证成功":
                    self.is_authenticated = True
                    logger.info("认证成功")
                    if self.on_authenticated:
                        await self.on_authenticated()
            
            elif msg_type in [MESSAGE_TYPE_SUBSCRIBE, MESSAGE_TYPE_BATCH_SUBSCRIBE, 
                             MESSAGE_TYPE_UNSUBSCRIBE, MESSAGE_TYPE_BATCH_UNSUBSCRIBE, 
                             MESSAGE_TYPE_RESET]:
                # 处理订阅相关的响应消息
                if self.on_success:
                    await self.on_success(msg_type, message.get("data"))
            
            elif msg_type == MESSAGE_TYPE_DATA:
                # 处理市场数据
                data = message.get("data", {})
                if isinstance(data, dict):
                    market_data = MarketData(
                        stock_code=data.get("stock_code", ""),
                        data_type=data.get("data_type", 0),
                        data=data.get("data"),
                        timestamp=data.get("timestamp", 0)
                    )
                    
                    if self.on_data:
                        await self.on_data(market_data)
            
            elif msg_type == MESSAGE_TYPE_ERROR:
                # 处理错误消息
                error_msg = message.get("error", "未知错误")
                logger.error(f"收到错误: {error_msg}")
                
                if self.on_error:
                    await self.on_error(error_msg)
            
            elif msg_type == MESSAGE_TYPE_PING:
                # 响应ping
                pong_msg = {
                    "type": MESSAGE_TYPE_PONG,
                    "timestamp": int(time.time() * 1000)
                }
                await self.send_message(pong_msg)
            
            elif msg_type == MESSAGE_TYPE_PONG:
                # 处理pong响应
                logger.debug("收到心跳响应")
            
            else:
                logger.warning(f"未知消息类型: {msg_type}")
        
        except json.JSONDecodeError:
            logger.error(f"JSON解析错误: {message_str}")
        except Exception as e:
            logger.error(f"处理消息错误: {e}")