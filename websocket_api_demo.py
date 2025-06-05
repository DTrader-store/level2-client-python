#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DTraderHQ WebSocket API 演示

这个示例展示如何直接使用websockets库连接到DTraderHQ服务器，
严格按照WebSocket API文档实现认证、订阅和数据接收功能。
"""

import asyncio
import json
import logging
import signal
import time
from typing import List, Dict, Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('websocket_client')


class DTraderWebSocketClient:
    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self.websocket = None
        self.connected = False
        self.authenticated = False
        self.subscribed_symbols = set()
        self.reconnect_interval = 5  # 重连间隔（秒）
        self.heartbeat_interval = 30  # 心跳间隔（秒）
        self.last_heartbeat = 0
        self.running = False
        self.tasks = []

    async def connect(self) -> None:
        """连接到WebSocket服务器"""
        while not self.connected and self.running:
            try:
                logger.info(f"正在连接到 {self.url}...")
                self.websocket = await websockets.connect(self.url)
                self.connected = True
                self.last_heartbeat = time.time()
                logger.info("连接成功！")
                
                # 连接后立即认证
                await self.authenticate()
                
                # 如果之前有订阅的股票，重新订阅
                if self.authenticated and self.subscribed_symbols:
                    symbols_to_resubscribe = list(self.subscribed_symbols)
                    await self.batch_subscribe(symbols_to_resubscribe)
            except Exception as e:
                logger.error(f"连接失败: {e}")
                await asyncio.sleep(self.reconnect_interval)

    async def authenticate(self) -> bool:
        """发送认证消息"""
        if not self.connected or not self.websocket:
            logger.error("未连接，无法认证")
            return False
            
        try:
            auth_message = {
                "type": "auth",
                "data": {
                    "token": self.token
                },
                "timestamp": int(time.time())
            }
            await self.websocket.send(json.dumps(auth_message))
            response = await self.websocket.recv()
            auth_response = json.loads(response)
            logger.info(f"认证响应: {auth_response}")
            
            if auth_response.get("type") == "auth" and auth_response.get("data", {}).get("message") == "认证成功":
                self.authenticated = True
                logger.info("认证成功！")
                return True
            else:
                logger.error(f"认证失败: {auth_response}")
                return False
        except Exception as e:
            logger.error(f"认证过程中出错: {e}")
            self.connected = False
            return False

    async def subscribe(self, stock_code: str, data_types: List[int]) -> bool:
        """订阅单个股票代码"""
        if not self.authenticated:
            logger.error("未认证，无法订阅")
            return False
            
        try:
            subscribe_message = {
                "type": "subscribe",
                "data": {
                    "stock_code": stock_code,
                    "data_types": data_types
                }
            }
            await self.websocket.send(json.dumps(subscribe_message))
            response = await self.websocket.recv()
            sub_response = json.loads(response)
            logger.info(f"订阅响应: {sub_response}")
            
            if sub_response.get("type") == "subscribe" and "订阅成功" in str(sub_response):
                self.subscribed_symbols.add(stock_code)
                return True
            else:
                logger.error(f"订阅失败: {sub_response}")
                return False
        except Exception as e:
            logger.error(f"订阅过程中出错: {e}")
            return False

    async def batch_subscribe(self, subscriptions: List[Dict[str, Any]]) -> bool:
        """批量订阅股票代码"""
        if not self.authenticated:
            logger.error("未认证，无法订阅")
            return False
            
        try:
            subscribe_message = {
                "type": "batch_subscribe",
                "data": {
                    "subscriptions": subscriptions
                }
            }
            await self.websocket.send(json.dumps(subscribe_message))
            response = await self.websocket.recv()
            sub_response = json.loads(response)
            logger.info(f"批量订阅响应: {sub_response}")
            
            if sub_response.get("type") == "batch_subscribe":
                for sub in subscriptions:
                    self.subscribed_symbols.add(sub["stock_code"])
                return True
            else:
                logger.error(f"批量订阅失败: {sub_response}")
                return False
        except Exception as e:
            logger.error(f"批量订阅过程中出错: {e}")
            return False

    async def unsubscribe(self, stock_code: str, data_types: List[int]) -> bool:
        """取消订阅单个股票代码"""
        if not self.authenticated:
            logger.error("未认证，无法取消订阅")
            return False
            
        try:
            unsubscribe_message = {
                "type": "unsubscribe",
                "data": {
                    "stock_code": stock_code,
                    "data_types": data_types
                }
            }
            await self.websocket.send(json.dumps(unsubscribe_message))
            response = await self.websocket.recv()
            unsub_response = json.loads(response)
            logger.info(f"取消订阅响应: {unsub_response}")
            
            if "成功" in str(unsub_response):
                if stock_code in self.subscribed_symbols:
                    self.subscribed_symbols.remove(stock_code)
                return True
            else:
                logger.error(f"取消订阅失败: {unsub_response}")
                return False
        except Exception as e:
            logger.error(f"取消订阅过程中出错: {e}")
            return False

    async def batch_unsubscribe(self, stock_codes: List[str]) -> bool:
        """批量取消订阅股票代码"""
        if not self.authenticated:
            logger.error("未认证，无法批量取消订阅")
            return False
            
        try:
            unsubscribe_message = {
                "type": "batch_unsubscribe",
                "data": {
                    "stock_codes": stock_codes
                }
            }
            await self.websocket.send(json.dumps(unsubscribe_message))
            response = await self.websocket.recv()
            unsub_response = json.loads(response)
            logger.info(f"批量取消订阅响应: {unsub_response}")
            
            if "成功" in str(unsub_response):
                for code in stock_codes:
                    if code in self.subscribed_symbols:
                        self.subscribed_symbols.remove(code)
                return True
            else:
                logger.error(f"批量取消订阅失败: {unsub_response}")
                return False
        except Exception as e:
            logger.error(f"批量取消订阅过程中出错: {e}")
            return False

    async def reset_subscriptions(self, subscriptions: List[Dict[str, Any]]) -> bool:
        """重置订阅（取消所有当前订阅并添加新订阅）"""
        if not self.authenticated:
            logger.error("未认证，无法重置订阅")
            return False
            
        try:
            reset_message = {
                "type": "reset",
                "data": {
                    "subscriptions": subscriptions
                },
                "timestamp": int(time.time())
            }
            await self.websocket.send(json.dumps(reset_message))
            response = await self.websocket.recv()
            reset_response = json.loads(response)
            logger.info(f"重置订阅响应: {reset_response}")
            
            if "成功" in str(reset_response):
                self.subscribed_symbols.clear()
                for sub in subscriptions:
                    self.subscribed_symbols.add(sub["stock_code"])
                return True
            else:
                logger.error(f"重置订阅失败: {reset_response}")
                return False
        except Exception as e:
            logger.error(f"重置订阅过程中出错: {e}")
            return False

    async def send_heartbeat(self) -> None:
        """发送心跳消息"""
        if not self.connected or not self.websocket:
            return
            
        try:
            heartbeat_message = {
                "type": "ping",
                "timestamp": int(time.time())
            }
            await self.websocket.send(json.dumps(heartbeat_message))
            self.last_heartbeat = time.time()
            logger.debug("已发送心跳")
        except Exception as e:
            logger.error(f"发送心跳失败: {e}")
            self.connected = False

    async def heartbeat_loop(self) -> None:
        """心跳循环"""
        while self.running:
            if self.connected and time.time() - self.last_heartbeat > self.heartbeat_interval:
                await self.send_heartbeat()
            await asyncio.sleep(1)

    async def process_message(self, message: Dict[str, Any]) -> None:
        """处理接收到的消息"""
        msg_type = message.get("type")
        
        if msg_type == "data":
            # 处理市场数据
            data = message.get("data", {})
            stock_code = data.get("stock_code")
            data_type = data.get("data_type")
            market_data = data.get("data", {})
            
            if data_type == 4:  # 逐笔成交
                logger.info(f"收到 {stock_code} 的逐笔成交数据: 价格={market_data.get('Price')}, 时间={market_data.get('Time')}, 数量={market_data.get('Volume')}")
            elif data_type == 14:  # 逐笔委托
                type_codes = market_data.get('Type', [])
                type_desc = ""
                if len(type_codes) >= 2:
                    if type_codes[0] == 66:
                        type_desc = "买入"
                    elif type_codes[0] == 83:
                        type_desc = "卖出"
                        
                    if type_codes[1] == 65:
                        type_desc += "报单"
                    elif type_codes[1] == 68:
                        type_desc += "撤单"
                        
                logger.info(f"收到 {stock_code} 的逐笔委托数据: 时间={market_data.get('DateTime')}, 价格={market_data.get('Price')}, 类型={type_desc}, 数量={market_data.get('Volume')}")
        elif msg_type == "pong":
            # 心跳响应
            logger.debug("收到心跳响应")
        elif msg_type == "error":
            # 错误消息
            logger.error(f"收到错误消息: {message.get('error')}")

    async def message_loop(self) -> None:
        """消息接收循环"""
        while self.running:
            if not self.connected:
                await self.connect()
                continue
                
            try:
                message = await self.websocket.recv()
                data = json.loads(message)
                await self.process_message(data)
            except ConnectionClosed:
                logger.warning("连接已关闭，尝试重新连接...")
                self.connected = False
                self.authenticated = False
            except Exception as e:
                logger.error(f"处理消息时出错: {e}")
                await asyncio.sleep(1)

    async def start(self) -> None:
        """启动客户端"""
        self.running = True
        
        # 创建任务
        message_task = asyncio.create_task(self.message_loop())
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        self.tasks = [message_task, heartbeat_task]
        
        # 等待任务完成
        await asyncio.gather(*self.tasks)

    async def stop(self) -> None:
        """停止客户端"""
        self.running = False
        
        # 如果已连接，尝试取消所有订阅
        if self.connected and self.authenticated and self.subscribed_symbols:
            await self.batch_unsubscribe(list(self.subscribed_symbols))
        
        # 关闭WebSocket连接
        if self.websocket:
            await self.websocket.close()
        
        # 取消所有任务
        for task in self.tasks:
            task.cancel()
        
        logger.info("客户端已停止")


async def main():
    # 服务器URL
    url = "ws://localhost:8080/ws"  # 替换为实际的服务器地址
    
    # 认证令牌
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxLCJ1c2VybmFtZSI6InNvb2JveSIsImV4cCI6MTc4MDE5MTMyNSwiaWF0IjoxNzQ5MDg3MzI1fQ.qwYExSfL2qz5G4u6rhXxPYr3wkezmwOCYb6OfL3tVZk"  # 替换为实际的认证令牌
    
    # 创建客户端
    client = DTraderWebSocketClient(url, token)
    
    # 设置信号处理
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(client.stop()))
    
    try:
        # 启动客户端
        logger.info("启动WebSocket客户端...")
        
        # 创建启动任务
        start_task = asyncio.create_task(client.start())
        
        # 等待连接和认证
        await asyncio.sleep(2)
        
        if client.authenticated:
            # 订阅示例股票
            subscriptions = [
                {"stock_code": "600468", "data_types": [4, 14]},
                {"stock_code": "600519", "data_types": [4, 14]}
            ]
            
            logger.info("批量订阅股票...")
            await client.batch_subscribe(subscriptions)
            
            # 运行一段时间后
            logger.info("客户端运行中，按Ctrl+C停止...")
            await asyncio.sleep(3600)  # 运行1小时
    except asyncio.CancelledError:
        logger.info("程序被取消")
    except Exception as e:
        logger.error(f"运行出错: {e}")
    finally:
        # 确保客户端正确停止
        await client.stop()


if __name__ == "__main__":
    # 运行异步函数
    asyncio.run(main())