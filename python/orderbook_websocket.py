"""
独立的订单簿WebSocket脚本
基于existing orderbook.py，专门为TET指标计算优化
"""

import asyncio
import aiohttp
import redis.asyncio as redis
import json
import time
import logging
import os
import sys
from datetime import datetime, timedelta
import numpy as np

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from core.config import DEFAULT_SYMBOLS
except ImportError:
    DEFAULT_SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT"]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderBookWebSocket:
    def __init__(self):
        # Convert symbol format (BTC/USDT -> BTCUSDT)
        self.symbols = [symbol.replace('/', '').upper() for symbol in DEFAULT_SYMBOLS]  # Limit to 5 symbols
        self.redis_db = 3  # Orderbook database
        self.ws_base_url = "wss://stream.binance.com:9443/ws"
        self.rest_api = "https://api.binance.com/api/v3/depth"
        self.data_retention_hours = 2  # Keep 2 hours of data

        # Proxy configuration
        self.http_proxy = os.environ.get('HTTP_PROXY', '')
        self.https_proxy = os.environ.get('HTTPS_PROXY', '')

        # Depth calculation ranges for TET indicators
        self.depth_ranges = [
            (0.005, 0.015),  # 0.5-1.5% range
            (0.01, 0.025),   # 1-2.5% range
            (0.02, 0.05)     # 2-5% range
        ]

    async def fetch_depth_snapshot(self, symbol, limit=1000):
        """Get orderbook snapshot based on existing logic"""
        session_kwargs = {}
        if self.http_proxy:
            session_kwargs['proxy'] = self.http_proxy

        async with aiohttp.ClientSession(**session_kwargs) as session:
            url = f"{self.rest_api}?symbol={symbol.upper()}&limit={limit}"
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"API request failed: {response.status}")

                snapshot = await response.json()
                return {
                    "bids": {float(price): float(quantity) for price, quantity in snapshot["bids"]},
                    "asks": {float(price): float(quantity) for price, quantity in snapshot["asks"]},
                    "lastUpdateId": snapshot["lastUpdateId"],
                    "current_price": float(snapshot["asks"][-1][0]) if snapshot["asks"] else 0
                }

    async def calculate_tet_orderbook_signals(self, redis_client, symbol, orderbook):
        """
        Calculate orderbook signals for TET based on existing pressure calculation logic
        """
        try:
            current_price = orderbook.get("current_price", 0)
            if current_price == 0:
                return

            bids = orderbook["bids"]
            asks = orderbook["asks"]

            # Calculate signals for different depth ranges
            for i, (low_pct, high_pct) in enumerate(self.depth_ranges):
                pressure, total_asks, total_bids = self.calculate_pressure_in_range(
                    bids, asks, current_price, low_pct, high_pct
                )

                # Convert to TET standard signal format
                minute_timestamp = int(time.time() // 60) * 60

                signal_data = {
                    'buy_volume': total_bids,
                    'sell_volume': total_asks,
                    'bid_depth': total_bids,
                    'ask_depth': total_asks,
                    'pressure': pressure,
                    'timestamp': minute_timestamp,
                    'current_price': current_price
                }

                # Store to Redis for TET indicator calculation
                # Normalize symbol in key to use underscore (e.g., BTC_USDT)
                norm_symbol = symbol.replace('/', '_')
                signal_key = f"orderbook_signals:{norm_symbol}:depth_{i}"
                await redis_client.zadd(signal_key, {json.dumps(signal_data): minute_timestamp})

                # Clean expired data
                cutoff_time = int((datetime.now() - timedelta(hours=self.data_retention_hours)).timestamp())
                await redis_client.zremrangebyscore(signal_key, 0, cutoff_time)

            # Log orderbook status
            if len(bids) > 0 and len(asks) > 0:
                best_bid = max(bids.keys())
                best_ask = min(asks.keys())
                spread = (best_ask - best_bid) / best_ask * 100
                logger.debug(f"{symbol}: 最佳买价${best_bid:.2f}, 最佳卖价${best_ask:.2f}, 点差{spread:.3f}%")

        except Exception as e:
            logger.error(f"{symbol}: 计算订单簿信号失败: {e}")

    def calculate_pressure_in_range(self, bids, asks, current_price, low_pct, high_pct):
        """
        Calculate bid/ask pressure in specified range based on existing alert_order_pct logic
        """
        if not bids or not asks or current_price == 0:
            return 0.0, 0.0, 0.0

        max_bid_price = max(bids.keys())
        min_ask_price = min(asks.keys())

        # Calculate bid range (below current price)
        bid_range = (max_bid_price * (1 - high_pct), max_bid_price * (1 - low_pct))

        # Calculate ask range (above current price)
        ask_range = (min_ask_price * (1 + low_pct), min_ask_price * (1 + high_pct))

        # Filter orders in range
        range_bids = [(price, amount) for price, amount in bids.items()
                      if bid_range[0] <= price <= bid_range[1]]
        range_asks = [(price, amount) for price, amount in asks.items()
                      if ask_range[0] <= price <= ask_range[1]]

        total_bids = sum(amount for price, amount in range_bids)
        total_asks = sum(amount for price, amount in range_asks)

        # Calculate pressure
        if total_bids + total_asks > 0:
            pressure = (total_bids - total_asks) / (total_bids + total_asks)
        else:
            pressure = 0.0

        return pressure, total_asks, total_bids

    async def subscribe_single_orderbook(self, symbol, redis_client):
        """Subscribe to single symbol orderbook"""
        orderbook = await self.fetch_depth_snapshot(symbol)
        last_calculation_minute = 0

        session_kwargs = {}
        if self.http_proxy:
            session_kwargs['proxy'] = self.http_proxy

        retry_count = 0
        max_retries = 5

        while retry_count <= max_retries:
            try:
                async with aiohttp.ClientSession(**session_kwargs) as session:
                    ws_url = f"{self.ws_base_url}/{symbol.lower()}@depth@1000ms"
                    async with session.ws_connect(ws_url) as websocket:
                        logger.info(f"{symbol}: 订单簿WebSocket连接已建立")
                        retry_count = 0  # Reset retry counter on successful connection

                        async for message in websocket:
                            try:
                                event = json.loads(message.data)
                                orderbook = self.update_orderbook(orderbook, event)

                                # Calculate TET signals every minute
                                current_minute = int(time.time() // 60)
                                if current_minute > last_calculation_minute:
                                    await self.calculate_tet_orderbook_signals(redis_client, symbol, orderbook)
                                    last_calculation_minute = current_minute

                            except json.JSONDecodeError:
                                continue
                            except Exception as e:
                                logger.error(f"{symbol}: 处理订单簿更新失败: {e}")

            except Exception as e:
                retry_count += 1
                logger.error(f"{symbol}: WebSocket连接错误 (尝试 {retry_count}/{max_retries}): {e}")
                if retry_count <= max_retries:
                    await asyncio.sleep(min(retry_count * 2, 30))  # Exponential backoff
                    # Re-fetch snapshot after connection error
                    try:
                        orderbook = await self.fetch_depth_snapshot(symbol)
                        logger.info(f"{symbol}: 重新获取订单簿快照")
                    except Exception as snapshot_error:
                        logger.error(f"{symbol}: 获取订单簿快照失败: {snapshot_error}")
                else:
                    logger.error(f"{symbol}: 达到最大重连次数，退出")
                    break

    def update_orderbook(self, orderbook, event):
        """Update orderbook based on existing process_orderbook_event logic"""
        # Simplified version maintaining core logic
        if event["u"] <= orderbook["lastUpdateId"]:
            return orderbook

        if event["U"] <= orderbook["lastUpdateId"] + 1 and event["u"] >= orderbook["lastUpdateId"] + 1:
            # Update bids
            for price, quantity in event["b"]:
                price, quantity = float(price), float(quantity)
                if quantity == 0:
                    orderbook["bids"].pop(price, None)
                else:
                    orderbook["bids"][price] = quantity

            # Update asks
            for price, quantity in event["a"]:
                price, quantity = float(price), float(quantity)
                if quantity == 0:
                    orderbook["asks"].pop(price, None)
                else:
                    orderbook["asks"][price] = quantity

            orderbook["lastUpdateId"] = event["u"]

            # Update current price (use best ask as reference)
            if orderbook["asks"]:
                orderbook["current_price"] = min(orderbook["asks"].keys())

        return orderbook

    async def run_orderbook_websockets(self):
        """Run orderbook WebSocket service for all symbols"""
        redis_client = await redis.from_url(f'redis://localhost:6388/{self.redis_db}')

        logger.info("订单簿WebSocket服务启动")
        logger.info(f"监控交易对: {self.symbols}")
        logger.info(f"深度范围: {self.depth_ranges}")
        logger.info(f"数据保留: {self.data_retention_hours}小时")

        try:
            tasks = []
            for symbol in self.symbols:
                task = asyncio.create_task(self.subscribe_single_orderbook(symbol, redis_client))
                tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)

        except KeyboardInterrupt:
            logger.info("收到停止信号，正在关闭...")
        finally:
            await redis_client.aclose()
            logger.info("订单簿WebSocket服务已停止")

async def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='TET订单簿WebSocket服务')
    parser.add_argument('--symbols', nargs='+', help='监控的交易对列表')
    parser.add_argument('--retention-hours', type=int, default=2, help='数据保留小时数')

    args = parser.parse_args()

    orderbook_ws = OrderBookWebSocket()

    # Apply command line arguments
    if args.symbols:
        orderbook_ws.symbols = [symbol.replace('/', '').upper() for symbol in args.symbols]
    if args.retention_hours != 2:
        orderbook_ws.data_retention_hours = args.retention_hours

    logger.info(f"启动配置: 交易对{orderbook_ws.symbols}, 数据保留{orderbook_ws.data_retention_hours}小时")

    await orderbook_ws.run_orderbook_websockets()

if __name__ == "__main__":
    asyncio.run(main())
