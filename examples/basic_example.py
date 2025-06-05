#!/usr/bin/env python3
"""
DTraderHQ Python客户端基本使用示例
"""

import asyncio
import logging
import signal
import sys
from typing import Dict, List

# 导入客户端
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from client import DTraderHQClient, MarketData

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BasicExample')


class StockDataClient:
    """股票数据客户端示例"""
    
    def __init__(self):
        self.client = DTraderHQClient(
            url="ws://localhost:8080/ws",
            ping_interval=30.0,
            timeout=10.0
        )
        
        # 设置事件处理器
        self.client.on_connected = self.on_connected
        self.client.on_disconnected = self.on_disconnected
        self.client.on_authenticated = self.on_authenticated
        self.client.on_data = self.on_market_data
        self.client.on_error = self.on_error
        self.client.on_success = self.on_success
        
        self.running = True
    
    async def on_connected(self) -> None:
        """连接成功回调"""
        logger.info("已连接到服务器")
    
    async def on_disconnected(self) -> None:
        """连接断开回调"""
        logger.info("与服务器断开连接")
    
    async def on_authenticated(self) -> None:
        """认证成功回调"""
        logger.info("认证成功，开始订阅股票数据")
        
        # 订阅股票数据
        try:
            # 单个订阅
            await self.client.subscribe("000001", [4, 8])  # 逐笔成交和逐笔大单
            await self.client.subscribe("000002", [4, 14])  # 逐笔成交和逐笔委托
            
            # 等待一段时间后进行批量订阅
            await asyncio.sleep(2)
            
            # 批量订阅
            batch_subscriptions = [
                {"stock_code": "600000", "data_types": [4, 8, 14]},  # 浦发银行
                {"stock_code": "600036", "data_types": [4]},         # 招商银行
                {"stock_code": "600519", "data_types": [4, 8]},      # 贵州茅台
            ]
            await self.client.batch_subscribe(batch_subscriptions)
            
            logger.info("订阅请求已发送")
        except Exception as e:
            logger.error(f"订阅失败: {e}")
    
    async def on_market_data(self, data: MarketData) -> None:
        """市场数据回调"""
        data_type_names = {
            4: "逐笔成交",
            8: "逐笔大单",
            14: "逐笔委托"
        }
        
        data_type_name = data_type_names.get(data.data_type, f"类型{data.data_type}")
        logger.info(f"收到数据: {data.stock_code} - {data_type_name}")
        logger.debug(f"数据内容: {data.data}")
    
    async def on_error(self, error: str) -> None:
        """错误回调"""
        logger.error(f"收到错误: {error}")
    
    async def on_success(self, msg_type: str, data: Dict) -> None:
        """成功回调"""
        logger.info(f"操作成功: {msg_type}")
        if data:
            logger.debug(f"响应数据: {data}")
    
    async def run(self) -> None:
        """运行客户端"""
        try:
            # 连接到服务器
            logger.info("正在连接到服务器...")
            await self.client.connect()
            
            # 认证（请替换为实际的token）
            token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxLCJ1c2VybmFtZSI6InNvb2JveSIsImV4cCI6MTc4MDE5MTMyNSwiaWF0IjoxNzQ5MDg3MzI1fQ.qwYExSfL2qz5G4u6rhXxPYr3wkezmwOCYb6OfL3tVZk"
            logger.info("正在进行认证...")
            await self.client.authenticate(token)
            
            # 保持运行
            logger.info("客户端运行中，按 Ctrl+C 退出...")
            while self.running:
                await asyncio.sleep(1)
                
                # 显示当前订阅状态
                if self.client.is_authenticated:
                    subscriptions = self.client.get_subscriptions()
                    if subscriptions:
                        logger.debug(f"当前订阅数量: {len(subscriptions)}")
        
        except KeyboardInterrupt:
            logger.info("收到退出信号")
        except Exception as e:
            logger.error(f"运行错误: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """清理资源"""
        logger.info("正在清理资源...")
        
        try:
            # 取消所有订阅
            if self.client.is_authenticated:
                subscriptions = self.client.get_subscriptions()
                if subscriptions:
                    stock_codes = list(subscriptions.keys())
                    logger.info(f"取消订阅 {len(stock_codes)} 只股票")
                    await self.client.batch_unsubscribe(stock_codes)
                    await asyncio.sleep(1)  # 等待取消订阅完成
        except Exception as e:
            logger.error(f"取消订阅失败: {e}")
        
        # 关闭连接
        await self.client.close()
        logger.info("客户端已关闭")
    
    def stop(self) -> None:
        """停止客户端"""
        self.running = False


def signal_handler(client: StockDataClient):
    """信号处理器"""
    def handler(signum, frame):
        logger.info(f"收到信号 {signum}，正在退出...")
        client.stop()
    return handler


async def main():
    """主函数"""
    client = StockDataClient()
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler(client))
    signal.signal(signal.SIGTERM, signal_handler(client))
    
    # 运行客户端
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被中断")
    except Exception as e:
        logger.error(f"程序错误: {e}")
        sys.exit(1)