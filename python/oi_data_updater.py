"""
独立的OI数据更新脚本
基于existing liquidation_listen-bn.py 的 fetch_oi_data 逻辑
"""

import asyncio
import redis.asyncio as redis
import ccxt.async_support as ccxt
import aiohttp
from datetime import datetime, timedelta
import logging
import json
import time
import os
import sys

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from core.config import DEFAULT_SYMBOLS
except ImportError:
    DEFAULT_SYMBOLS = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
        "SOL/USDT", "DOGE/USDT", "DOT/USDT", "UNI/USDT", "LINK/USDT"
    ]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OIDataUpdater:
    def __init__(self):
        self.update_interval = 300  # 5-minute update interval
        self.symbols = DEFAULT_SYMBOLS
        self.redis_db = 0  # OI database
        self.timeframe = "5m"
        self.history_hours = 2  # Fetch 2 hours of history

        # Binance API configuration (using environment or defaults)
        self.api_key = os.environ.get('BN_API_KEY', '')
        self.api_secret = os.environ.get('BN_API_SECRET', '')
        self.http_proxy = os.environ.get('HTTP_PROXY', '')
        self.https_proxy = os.environ.get('HTTPS_PROXY', '')

    async def create_exchange(self):
        """Create exchange instance based on existing logic"""
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))

        exchange_config = {
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'aiohttp_proxy': self.http_proxy,
            'aiohttpClient': session,
            'rateLimit': 1200,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            },
        }

        exchange = ccxt.binance(exchange_config)
        return session, exchange

    def to_iso(self, timestamp):
        """Convert timestamp to ISO format"""
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')

    async def fetch_oi_data(self, exchange, symbol, redis_client, limit=100):
        """
        Fetch OI data based on existing liquidation_listen-bn.py logic
        """
        try:
            redis_key = f"open_interest:{symbol}"
            before_time = datetime.now() - timedelta(hours=self.history_hours)

            # Fetch open interest history
            open_interest = await exchange.fetch_open_interest_history(
                symbol=symbol,
                timeframe=self.timeframe,
                since=exchange.parse8601(self.to_iso(before_time.timestamp())),
                limit=limit
            )

            # Store to Redis
            for oi_data in open_interest:
                await redis_client.zadd(redis_key, {str(oi_data): oi_data["timestamp"]})

            # Clean old data (keep only 24 hours)
            cutoff_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
            await redis_client.zremrangebyscore(redis_key, 0, cutoff_time)

            logger.info(f"{symbol}: OI data updated, {len(open_interest)} records")
            return True

        except Exception as e:
            logger.error(f"Error fetching OI for {symbol}: {e}")
            return False

    async def update_all_symbols(self, exchange, redis_client):
        """Update OI data for all symbols"""
        tasks = []
        for symbol in self.symbols:
            task = self.fetch_oi_data(exchange, symbol, redis_client)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)

        logger.info(f"OI更新完成: {success_count}/{len(self.symbols)} 成功")
        return success_count

    async def run_continuous(self):
        """Continuously run OI data update service"""
        # Create exchange and Redis connections
        session, exchange = await self.create_exchange()
        redis_client = await redis.from_url(f'redis://localhost:6388/{self.redis_db}')

        logger.info("OI数据更新服务启动")
        logger.info(f"更新间隔: {self.update_interval}秒")
        logger.info(f"监控交易对: {len(self.symbols)}个")
        logger.info(f"时间框架: {self.timeframe}")

        try:
            while True:
                try:
                    start_time = time.time()

                    # Update all symbols
                    success_count = await self.update_all_symbols(exchange, redis_client)

                    # Log performance
                    elapsed_time = time.time() - start_time
                    logger.info(f"更新耗时: {elapsed_time:.2f}秒")

                    # Wait for next update
                    wait_time = max(0, self.update_interval - elapsed_time)
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

                except Exception as e:
                    logger.error(f"OI更新循环出错: {e}")
                    await asyncio.sleep(60)  # Wait 1 minute on error

        except KeyboardInterrupt:
            logger.info("收到停止信号，正在关闭...")
        finally:
            await exchange.close()
            await session.close()
            await redis_client.aclose()
            logger.info("OI数据更新服务已停止")

async def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='TET OI数据更新服务')
    parser.add_argument('--interval', type=int, default=300, help='更新间隔（秒）')
    parser.add_argument('--timeframe', type=str, default='5m', help='时间框架')

    args = parser.parse_args()

    updater = OIDataUpdater()

    # Apply command line arguments
    if args.interval != 300:
        updater.update_interval = args.interval
    if args.timeframe != '5m':
        updater.timeframe = args.timeframe

    logger.info(f"启动配置: 间隔{updater.update_interval}秒, 时间框架{updater.timeframe}")

    await updater.run_continuous()

if __name__ == "__main__":
    asyncio.run(main())