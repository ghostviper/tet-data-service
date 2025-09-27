"""
独立的清算数据WebSocket脚本
基于existing liquidation_listen-bn.py，专门为TET指标计算服务
"""

import asyncio
import aiohttp
import redis.asyncio as redis
import json
import logging
import os
import sys
from datetime import datetime, timedelta
import numpy as np

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set proxy environment variables (参考原始实现)
try:
    from core.config import HTTP_PROXY, HTTPS_PROXY, DEFAULT_SYMBOLS
    if HTTP_PROXY:
        os.environ["http_proxy"] = HTTP_PROXY
        os.environ["https_proxy"] = HTTPS_PROXY
        logger.info(f"设置代理: {HTTP_PROXY}")
except ImportError:
    HTTP_PROXY = os.environ.get('HTTP_PROXY', '')
    HTTPS_PROXY = os.environ.get('HTTPS_PROXY', '')
    DEFAULT_SYMBOLS = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
        "SOL/USDT", "DOGE/USDT", "DOT/USDT", "UNI/USDT", "LINK/USDT"
    ]
    logger.warning("无法导入代理配置，将使用环境变量或默认设置")

class LiquidationWebSocket:
    def __init__(self):
        self.liquidation_url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        self.redis_db = 1  # Liquidation database
        self.data_retention_hours = 24  # Keep 24 hours of data

        # Proxy configuration (参考working implementation)
        self.http_proxy = HTTP_PROXY
        self.https_proxy = HTTPS_PROXY

        # Min liquidation thresholds for different symbols
        self.min_liquidation_thresholds = {
            'BTCUSDT': 50000,    # $50k threshold for BTC
            'ETHUSDT': 30000,    # $30k threshold for ETH
            'default': 10000     # $10k threshold for other symbols
        }

    async def process_liquidation_for_tet(self, redis_client, liq_event):
        """
        Process liquidation events for TET indicator calculations
        Based on existing logic but optimized for TET framework
        """
        try:
            symbol = liq_event["o"]["s"]
            direction = liq_event["o"]["S"]  # SELL=long liquidation, BUY=short liquidation
            quantity = float(liq_event["o"]["q"])
            price = float(liq_event["o"]["ap"])
            liquidation_value = quantity * price
            timestamp = liq_event["E"]

            # Store original liquidation event
            redis_key = f"liquidation:{symbol}"
            await redis_client.zadd(redis_key, {str(liq_event): timestamp})

            # Prepare aggregated data for TET indicator calculation
            await self.prepare_tet_liquidation_data(redis_client, symbol, direction, liquidation_value, timestamp)

            # Log significant liquidations
            threshold = self.min_liquidation_thresholds.get(symbol, self.min_liquidation_thresholds['default'])
            if liquidation_value > threshold:
                direction_text = "多头爆仓" if direction == "SELL" else "空头爆仓"
                logger.info(f"{symbol}: {direction_text} ${liquidation_value:,.0f}")

            # Clean expired data
            cutoff_time = int((datetime.now() - timedelta(hours=self.data_retention_hours)).timestamp() * 1000)
            await redis_client.zremrangebyscore(redis_key, 0, cutoff_time)

        except Exception as e:
            logger.error(f"处理清算事件失败: {e}")

    async def prepare_tet_liquidation_data(self, redis_client, symbol, direction, liquidation_value, timestamp):
        """
        Prepare aggregated liquidation data for TET indicator calculation
        Separate long and short liquidations, aggregated by minute
        """
        minute_timestamp = (timestamp // 60000) * 60000  # Align to minute

        # Long liquidation data (SELL orders = longs being liquidated)
        if direction == "SELL":
            long_key = f"liquidation_long:{symbol}"
            await redis_client.zincrby(long_key, liquidation_value, minute_timestamp)

        # Short liquidation data (BUY orders = shorts being liquidated)
        else:
            short_key = f"liquidation_short:{symbol}"
            await redis_client.zincrby(short_key, liquidation_value, minute_timestamp)

        # Clean expired aggregated data
        cutoff_time = int((datetime.now() - timedelta(hours=self.data_retention_hours)).timestamp() * 1000)
        await redis_client.zremrangebyscore(f"liquidation_long:{symbol}", 0, cutoff_time)
        await redis_client.zremrangebyscore(f"liquidation_short:{symbol}", 0, cutoff_time)

    async def detect_liquidation_cascades(self, redis_client, symbol, window_minutes=15):
        """
        Detect liquidation cascades for cascade indicator
        Based on existing anomaly detection logic
        """
        try:
            # Get recent liquidation data
            end_time = int(datetime.now().timestamp() * 1000)
            start_time = end_time - (window_minutes * 60 * 1000)

            # Get long liquidations
            long_data = await redis_client.zrangebyscore(
                f"liquidation_long:{symbol}",
                start_time, end_time,
                withscores=True
            )

            # Get short liquidations
            short_data = await redis_client.zrangebyscore(
                f"liquidation_short:{symbol}",
                start_time, end_time,
                withscores=True
            )

            # Calculate cascade indicators (handle both string and bytes data)
            total_long_liq = sum(
                float(value.decode('utf-8') if isinstance(value, bytes) else value)
                for value, _ in long_data
            )
            total_short_liq = sum(
                float(value.decode('utf-8') if isinstance(value, bytes) else value)
                for value, _ in short_data
            )
            total_liquidation = total_long_liq + total_short_liq

            # Store cascade data for TET indicators
            if total_liquidation > 50000:  # Significant liquidation threshold
                cascade_data = {
                    'total_liquidation': total_liquidation,
                    'long_liquidation': total_long_liq,
                    'short_liquidation': total_short_liq,
                    'cascade_ratio': total_short_liq / total_liquidation if total_liquidation > 0 else 0,
                    'timestamp': end_time
                }

                cascade_key = f"liquidation_cascade:{symbol}"
                await redis_client.zadd(cascade_key, {json.dumps(cascade_data): end_time})

                # Clean old cascade data
                cutoff_time = int((datetime.now() - timedelta(hours=2)).timestamp() * 1000)
                await redis_client.zremrangebyscore(cascade_key, 0, cutoff_time)

                logger.info(f"{symbol}: 清算级联检测 - 总额${total_liquidation:,.0f}, 比率{cascade_data['cascade_ratio']:.2f}")

        except Exception as e:
            logger.error(f"{symbol}: 清算级联检测失败: {e}")

    async def run_liquidation_websocket(self):
        """Run liquidation data WebSocket service"""
        redis_client = await redis.from_url(f'redis://localhost:6388/{self.redis_db}')

        retries = 0
        max_retries = 5

        logger.info("清算WebSocket服务启动")
        logger.info(f"连接地址: {self.liquidation_url}")
        logger.info(f"数据保留: {self.data_retention_hours}小时")
        logger.info(f"代理设置: {self.http_proxy}")

        while retries <= max_retries:
            try:
                # 参考working implementation的连接方式
                async with aiohttp.ClientSession().ws_connect(self.liquidation_url, proxy=self.http_proxy) as websocket:
                    logger.info("清算WebSocket连接已建立")
                    retries = 0  # Reset retry counter on successful connection

                    # Start cascade detection task
                    cascade_task = asyncio.create_task(self.periodic_cascade_detection(redis_client))

                    try:
                        async for message in websocket:
                            try:
                                data = json.loads(message.data)
                                await self.process_liquidation_for_tet(redis_client, data)

                            except json.JSONDecodeError:
                                continue
                            except Exception as e:
                                logger.error(f"处理清算消息失败: {e}")
                                continue
                    finally:
                        cascade_task.cancel()

            except Exception as e:
                retries += 1
                logger.error(f"清算WebSocket连接错误 (尝试 {retries}/{max_retries}): {e}")
                if retries <= max_retries:
                    await asyncio.sleep(min(retries * 2, 30))  # Exponential backoff
                else:
                    logger.error("达到最大重连次数，退出")
                    break

        await redis_client.aclose()  # 使用aclose()而不是close()

    async def periodic_cascade_detection(self, redis_client):
        """Periodically detect liquidation cascades"""
        try:
            # Get active symbols from configuration
            symbols = [symbol.replace('/', '') + 'T' for symbol in DEFAULT_SYMBOLS]  # Convert to Binance format

            while True:
                for symbol in symbols:
                    await self.detect_liquidation_cascades(redis_client, symbol)

                await asyncio.sleep(60)  # Check every minute

        except asyncio.CancelledError:
            logger.info("级联检测任务已取消")
        except Exception as e:
            logger.error(f"级联检测任务错误: {e}")

async def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='TET清算数据WebSocket服务')
    parser.add_argument('--retention-hours', type=int, default=24, help='数据保留小时数')

    args = parser.parse_args()

    liquidation_ws = LiquidationWebSocket()

    # Apply command line arguments
    if args.retention_hours != 24:
        liquidation_ws.data_retention_hours = args.retention_hours

    logger.info(f"启动配置: 数据保留{liquidation_ws.data_retention_hours}小时")

    try:
        await liquidation_ws.run_liquidation_websocket()
    except KeyboardInterrupt:
        logger.info("收到停止信号，正在关闭...")

if __name__ == "__main__":
    asyncio.run(main())