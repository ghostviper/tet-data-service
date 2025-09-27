"""
Volatility Monitor for TET Framework
币种波动性监控脚本 - 计算ATR和IO指标，利用Redis数据存储
"""

import asyncio
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys
import os
import time
from asyncio import Semaphore
from io import StringIO
import math

# Add project root and core to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'core'))

from core.data_processor import CryptoDataProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VolatilityAnalyzer:
    """波动性分析器 - 计算ATR和IO指标"""

    @staticmethod
    def calculate_true_range(high: pd.Series, low: pd.Series, close_prev: pd.Series) -> pd.Series:
        """计算真实波幅 True Range"""
        # TR = max(High - Low, abs(High - Close_prev), abs(Low - Close_prev))
        hl_diff = high - low
        hc_diff = np.abs(high - close_prev)
        lc_diff = np.abs(low - close_prev)

        true_range = np.maximum(hl_diff, np.maximum(hc_diff, lc_diff))
        return pd.Series(true_range, index=high.index)

    @staticmethod
    def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """计算平均真实波幅 ATR"""
        # 计算前一日收盘价
        close_prev = close.shift(1)

        # 计算真实波幅
        true_range = VolatilityAnalyzer.calculate_true_range(high, low, close_prev)

        # 计算ATR（使用EMA平滑）
        atr = true_range.ewm(span=period, adjust=False).mean()

        return atr

    @staticmethod
    def calculate_atr_percentage(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """计算ATR百分比"""
        atr = VolatilityAnalyzer.calculate_atr(high, low, close, period)
        atr_percentage = (atr / close) * 100
        return atr_percentage

    @staticmethod
    def calculate_price_change(close: pd.Series, period: int = 1) -> pd.Series:
        """计算价格涨跌幅 (IO - In/Out)"""
        price_change = ((close - close.shift(period)) / close.shift(period)) * 100
        return price_change

    def calculate_volatility_metrics(self, df: pd.DataFrame) -> Dict:
        """计算完整的波动性指标"""
        if len(df) < 10:  # 确保有足够的数据
            return None

        # 自动检测数据时间周期
        timeframe_minutes = self._detect_data_timeframe(df)
        logger.debug(f"检测到数据周期: {timeframe_minutes}分钟")

        # 根据检测到的周期计算各分析周期
        periods = self._calculate_periods_for_timeframe(timeframe_minutes)

        # 确保有足够的数据进行分析
        min_required_data = max(periods.values()) * 2
        if len(df) < min_required_data:
            logger.warning(f"数据不足，需要至少{min_required_data}条数据，当前有{len(df)}条")
            return None

        high = df['high']
        low = df['low']
        close = df['close']

        # 使用动态计算的周期
        atr_periods = periods

        # 计算ATR指标
        atr_metrics = {}
        for period_name, period_candles in atr_periods.items():
            if len(df) >= period_candles * 2:  # 确保有足够数据
                atr = VolatilityAnalyzer.calculate_atr(high, low, close, period_candles)
                atr_pct = VolatilityAnalyzer.calculate_atr_percentage(high, low, close, period_candles)

                atr_metrics[f'atr_{period_name}'] = atr.iloc[-1] if not atr.empty else 0
                atr_metrics[f'atr_{period_name}_pct'] = atr_pct.iloc[-1] if not atr_pct.empty else 0

        # 计算价格变化 (IO指标) - 使用动态周期
        io_periods = periods

        io_metrics = {}
        for period_name, period_candles in io_periods.items():
            if len(df) >= period_candles + 1:
                io_change = VolatilityAnalyzer.calculate_price_change(close, period_candles)
                io_metrics[f'io_{period_name}'] = io_change.iloc[-1] if not io_change.empty else 0

        # 当前价格和基础信息 - 使用动态周期
        current_price = close.iloc[-1]
        volume_24h_periods = periods['24h']
        volume_24h = df['volume'].tail(volume_24h_periods).sum() if len(df) >= volume_24h_periods else df['volume'].sum()

        # 计算换手率指标（如果有持仓量数据）
        turnover_metrics = self.calculate_turnover_metrics(df, timeframe_minutes)

        # 波动性等级评估（包含换手率）
        volatility_score = VolatilityAnalyzer.calculate_volatility_score(
            atr_metrics, io_metrics, turnover_metrics
        )

        result = {
            'current_price': current_price,
            'volume_24h': volume_24h,
            'volatility_score': volatility_score,
            'timeframe_minutes': timeframe_minutes,  # 添加检测到的时间周期信息
            **atr_metrics,
            **io_metrics,
            'calculated_at': datetime.now().isoformat(),
            'data_points': len(df)
        }

        # 如果有换手率数据，添加到结果中
        if turnover_metrics:
            result.update(turnover_metrics)

        return result

    @staticmethod
    def calculate_turnover_rate_raw(volume: pd.Series, open_interest: pd.Series) -> pd.Series:
        """计算原始换手率 Volume / OI"""
        # 避免除零错误
        oi_safe = open_interest.replace(0, np.nan)
        turnover_raw = volume / oi_safe
        return turnover_raw.fillna(0)

    @staticmethod
    def calculate_turnover_rate_normalized(volume: pd.Series, open_interest: pd.Series) -> pd.Series:
        """
        计算归一化换手率，映射到 [-0.5, 0.5] 区间

        步骤：
        1. R_raw = Volume / OI
        2. R = log(1 + R_raw)  # 对数压缩
        3. R_norm = R / (1 + R)  # 归一化到 [0, 1]
        4. TurnoverRate = R_norm - 0.5  # 映射到 [-0.5, 0.5]
        """
        # 计算原始换手率
        r_raw = VolatilityAnalyzer.calculate_turnover_rate_raw(volume, open_interest)

        # 对数压缩（避免极端值）
        r_log = np.log(1 + np.abs(r_raw))

        # 归一化到 [0, 1]
        r_norm = r_log / (1 + r_log)

        # 映射到 [-0.5, 0.5]，保持原始符号
        turnover_normalized = np.where(r_raw >= 0, r_norm - 0.5, -(r_norm - 0.5))

        return pd.Series(turnover_normalized, index=volume.index)

    def calculate_turnover_metrics(self, df: pd.DataFrame, timeframe_minutes: int = None) -> Dict:
        """计算换手率相关指标"""
        if 'open_interest' not in df.columns or len(df) < 2:
            return {}

        volume = df['volume']
        open_interest = df['open_interest']

        # 如果没有提供时间周期，自动检测
        if timeframe_minutes is None:
            timeframe_minutes = self._detect_data_timeframe(df)

        # 根据数据时间周期计算各分析周期
        periods = self._calculate_periods_for_timeframe(timeframe_minutes)

        turnover_metrics = {}

        for period_name, period_candles in periods.items():
            if len(df) >= period_candles:
                # 计算该周期内的累计成交量和平均持仓量
                period_volume = volume.rolling(window=period_candles).sum()
                period_oi_avg = open_interest.rolling(window=period_candles).mean()

                # 原始换手率
                raw_turnover = VolatilityAnalyzer.calculate_turnover_rate_raw(
                    period_volume, period_oi_avg
                )

                # 归一化换手率
                normalized_turnover = VolatilityAnalyzer.calculate_turnover_rate_normalized(
                    period_volume, period_oi_avg
                )

                # 取最新值
                turnover_metrics[f'turnover_{period_name}_raw'] = raw_turnover.iloc[-1] if not raw_turnover.empty else 0
                turnover_metrics[f'turnover_{period_name}_norm'] = normalized_turnover.iloc[-1] if not normalized_turnover.empty else 0

        # 当前持仓量
        current_oi = open_interest.iloc[-1] if not open_interest.empty else 0
        turnover_metrics['current_open_interest'] = current_oi

        return turnover_metrics

    @staticmethod
    def calculate_turnover_level(turnover_raw: float) -> Dict:
        """根据原始换手率计算换手水平"""
        if turnover_raw >= 5.0:
            level = 'EXTREME'  # 极高换手
            score = 100
        elif turnover_raw >= 3.0:
            level = 'HIGH'     # 高换手
            score = 80
        elif turnover_raw >= 1.5:
            level = 'MEDIUM'   # 中等换手
            score = 60
        elif turnover_raw >= 0.8:
            level = 'LOW'      # 低换手
            score = 40
        else:
            level = 'MINIMAL'  # 极低换手
            score = 20

        return {
            'level': level,
            'score': score
        }

    @staticmethod
    def calculate_volatility_score(atr_metrics: Dict, io_metrics: Dict, turnover_metrics: Dict = None) -> Dict:
        """计算波动性评分"""
        # ATR评分 (基于ATR百分比)
        atr_24h_pct = atr_metrics.get('atr_24h_pct', 0)

        if atr_24h_pct >= 8:
            atr_level = 'EXTREME'  # 极高波动
            atr_score = 100
        elif atr_24h_pct >= 5:
            atr_level = 'HIGH'     # 高波动
            atr_score = 80
        elif atr_24h_pct >= 3:
            atr_level = 'MEDIUM'   # 中等波动
            atr_score = 60
        elif atr_24h_pct >= 1.5:
            atr_level = 'LOW'      # 低波动
            atr_score = 40
        else:
            atr_level = 'MINIMAL'  # 极低波动
            atr_score = 20

        # IO评分 (基于24小时涨跌幅)
        io_24h = abs(io_metrics.get('io_24h', 0))

        if io_24h >= 15:
            io_level = 'EXTREME'
            io_score = 100
        elif io_24h >= 10:
            io_level = 'HIGH'
            io_score = 80
        elif io_24h >= 5:
            io_level = 'MEDIUM'
            io_score = 60
        elif io_24h >= 2:
            io_level = 'LOW'
            io_score = 40
        else:
            io_level = 'MINIMAL'
            io_score = 20

        # 换手率评分
        turnover_level = 'N/A'
        turnover_score = 0

        if turnover_metrics:
            turnover_24h_raw = turnover_metrics.get('turnover_24h_raw', 0)
            turnover_info = VolatilityAnalyzer.calculate_turnover_level(turnover_24h_raw)
            turnover_level = turnover_info['level']
            turnover_score = turnover_info['score']

        # 综合评分 (如果有换手率数据，则三者平均；否则仍然使用ATR和IO)
        if turnover_metrics:
            overall_score = (atr_score + io_score + turnover_score) / 3
        else:
            overall_score = (atr_score + io_score) / 2

        if overall_score >= 90:
            overall_level = 'EXTREME'
        elif overall_score >= 70:
            overall_level = 'HIGH'
        elif overall_score >= 50:
            overall_level = 'MEDIUM'
        elif overall_score >= 30:
            overall_level = 'LOW'
        else:
            overall_level = 'MINIMAL'

        result = {
            'atr_level': atr_level,
            'atr_score': atr_score,
            'io_level': io_level,
            'io_score': io_score,
            'overall_level': overall_level,
            'overall_score': overall_score
        }

        # 如果有换手率数据，添加到结果中
        if turnover_metrics:
            result.update({
                'turnover_level': turnover_level,
                'turnover_score': turnover_score
            })

        return result

class VolatilityMonitor(VolatilityAnalyzer):
    """波动性监控器主类"""

    def __init__(self):
        super().__init__()
        self.data_processor = None

        # 监控参数
        self.update_interval = 300  # 5分钟更新一次波动性指标
        self.concurrent_requests = 10
        self.semaphore = Semaphore(self.concurrent_requests)
        self.is_running = False
        self.display_top_count = 15  # 默认显示数量

        # 从配置文件导入交易对列表
        try:
            from core.config import DEFAULT_SYMBOLS
            self.symbols = DEFAULT_SYMBOLS
            logger.info(f"已从配置文件加载 {len(self.symbols)} 个交易对")
        except ImportError:
            self.symbols = [
                "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
                "SOL/USDT", "DOGE/USDT", "DOT/USDT", "UNI/USDT", "LINK/USDT",
                "AVAX/USDT", "MEME/USDT", "PUMP/USDT", "PEOPLE/USDT", "BOME/USDT",
                "ARB/USDT", "MATIC/USDT", "FTM/USDT", "SAND/USDT", "AXS/USDT",
                "NEAR/USDT", "ALGO/USDT", "XLM/USDT", "LIT/USDT", "MKR/USDT"
            ]
            logger.warning("配置文件导入失败，使用默认交易对列表")

    async def __aenter__(self):
        self.data_processor = CryptoDataProcessor()
        await self.data_processor.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.data_processor:
            await self.data_processor.cleanup()

    def _get_kline_redis_key(self, symbol: str, timeframe: str = '15m') -> str:
        """获取K线数据Redis键名（与real_time_data_updater格式一致）"""
        return f"tet:kline:{timeframe}:{symbol.replace('/', '_')}"

    def _get_volatility_redis_key(self, symbol: str) -> str:
        """获取波动性数据Redis键名"""
        return f"tet:volatility:{symbol.replace('/', '_')}"

    def _get_volatility_ranking_key(self) -> str:
        """获取波动性排行榜键名"""
        return "tet:volatility:ranking"


    def _detect_data_timeframe(self, df: pd.DataFrame) -> int:
        """
        自动检测数据的时间周期（返回分钟数）

        通过分析时间索引的间隔来判断数据周期
        """
        if len(df) < 2:
            return 60  # 默认1小时

        # 计算前几个时间间隔的中位数
        time_diffs = []
        for i in range(1, min(10, len(df))):  # 检查前10个间隔
            diff = (df.index[i] - df.index[i-1]).total_seconds() / 60
            time_diffs.append(diff)

        if not time_diffs:
            return 60

        median_diff = sorted(time_diffs)[len(time_diffs) // 2]

        # 判断最接近的标准时间周期
        if median_diff <= 20:
            return 15  # 15分钟
        elif median_diff <= 40:
            return 30  # 30分钟
        elif median_diff <= 90:
            return 60  # 1小时
        elif median_diff <= 180:
            return 120  # 2小时
        elif median_diff <= 360:
            return 240  # 4小时
        else:
            return 1440  # 1天

    def _calculate_periods_for_timeframe(self, timeframe_minutes: int) -> Dict:
        """
        根据数据时间周期计算各种分析周期

        Args:
            timeframe_minutes: 数据时间周期（分钟）

        Returns:
            包含各种分析周期的字典
        """
        # 计算各时间周期需要的K线数量
        periods = {
            '1h': max(1, 60 // timeframe_minutes),
            '4h': max(1, 240 // timeframe_minutes),
            '24h': max(1, 1440 // timeframe_minutes)
        }

        return periods

    async def load_market_data(self, symbol: str, fallback_timeframe: str = '1h', fallback_days: int = 30) -> Optional[pd.DataFrame]:
        """优先从Redis加载市场数据，失败时使用API作为fallback"""
        try:
            # 首先尝试从Redis获取数据（优先使用15m数据）
            redis_key = self._get_kline_redis_key(symbol, '15m')
            cached_data = await self.data_processor.redis_client.get(redis_key)

            if cached_data:
                data_info = json.loads(cached_data)

                # 检查数据是否过期（超过10分钟）
                last_update = datetime.fromisoformat(data_info['last_update'])
                if datetime.now() - last_update <= timedelta(minutes=10):
                    # 解析OHLCV数据
                    df = pd.read_json(StringIO(data_info['ohlcv_data']), orient='index')
                    df.index = pd.to_datetime(df.index)
                    df.sort_index(inplace=True)

                    if len(df) >= 10:
                        logger.debug(f"{symbol}: 从Redis获取{len(df)}条K线数据")
                        return df
                    else:
                        logger.warning(f"{symbol}: Redis数据不足({len(df)}条)")
                else:
                    age_minutes = (datetime.now() - last_update).total_seconds() / 60
                    logger.warning(f"{symbol}: Redis数据过期({age_minutes:.1f}分钟)")

            # Redis数据不可用，使用API作为fallback
            logger.info(f"{symbol}: Redis数据不可用，使用API获取数据")
            return await self._fetch_from_api_fallback(symbol, fallback_timeframe, fallback_days)

        except Exception as e:
            logger.error(f"{symbol}: 加载市场数据失败: {e}")
            # 尝试API fallback
            try:
                return await self._fetch_from_api_fallback(symbol, fallback_timeframe, fallback_days)
            except Exception as fallback_error:
                logger.error(f"{symbol}: API fallback也失败: {fallback_error}")
                return None

    async def _fetch_from_api_fallback(self, symbol: str, timeframe: str, lookback_days: int) -> Optional[pd.DataFrame]:
        """API fallback方法"""
        try:
            # 直接获取历史数据
            data = await self.data_processor.fetch_historical_data(
                symbol, lookback_days, timeframe
            )

            if data.empty or len(data) < 10:
                logger.warning(f"{symbol}: API数据不足，获取到{len(data)}条记录")
                return None

            # 处理市场数据
            processed_data = self.data_processor.process_market_data(data)

            logger.debug(f"{symbol}: API获取{len(processed_data)}条{timeframe}数据")
            return processed_data

        except Exception as e:
            logger.error(f"{symbol}: API获取数据失败: {e}")
            return None

    async def fetch_open_interest_data(self, symbol: str, df: pd.DataFrame) -> Optional[pd.Series]:
        """
        基于成交量估算持仓量数据

        Args:
            symbol: 交易对符号
            df: 市场数据DataFrame
        """
        try:
            if df is None or len(df) < 10:
                return None

            # 自动检测数据周期并计算24小时平均成交量
            timeframe_minutes = self._detect_data_timeframe(df)
            periods_24h = max(1, 1440 // timeframe_minutes)  # 24小时需要的K线数量

            volume = df['volume']
            avg_volume = volume.rolling(window=min(periods_24h, len(df))).mean()

            # 估算持仓量（通常是成交量的2-5倍）
            # 对于不同类型的币种使用不同的倍数
            if symbol in ['BTC/USDT', 'ETH/USDT']:
                multiplier = 3.0  # 主流币
            elif 'USDT' in symbol:
                multiplier = 2.5  # 其他币种
            else:
                multiplier = 2.0  # 小币种

            # 添加一些随机波动（±20%）
            np.random.seed(hash(symbol) % 2**32)
            fluctuations = np.random.normal(1.0, 0.1, len(df))

            estimated_oi = avg_volume * multiplier * fluctuations

            # 确保持仓量为正数
            estimated_oi = np.maximum(estimated_oi, avg_volume * 0.5)

            oi_series = pd.Series(estimated_oi, index=df.index)

            logger.debug(f"{symbol}: 估算持仓量 - 数据周期: {timeframe_minutes}分钟, 24h需要: {periods_24h}个K线, 倍数: {multiplier}x")
            return oi_series

        except Exception as e:
            logger.debug(f"{symbol}: 估算持仓量失败: {e}")
            return None


    async def calculate_volatility_for_symbol(self, symbol: str, fallback_timeframe: str = '1h', fallback_days: int = 30) -> Optional[Dict]:
        """为单个交易对计算波动性指标"""
        try:
            # 优先从Redis获取数据，失败时使用API fallback
            df = await self.load_market_data(symbol, fallback_timeframe, fallback_days)
            if df is None or len(df) < 10:
                logger.warning(f"{symbol}: 数据不足或无法加载")
                return None

            # 尝试获取持仓量数据
            try:
                oi_series = await self.fetch_open_interest_data(symbol, df)
                if oi_series is not None and len(oi_series) == len(df):
                    df['open_interest'] = oi_series
                    logger.debug(f"{symbol}: 成功添加估算持仓量数据")
            except Exception as oi_error:
                logger.debug(f"{symbol}: 获取持仓量数据失败: {oi_error}")

            # 计算波动性指标（包含换手率如果有持仓量数据）
            volatility_metrics = self.calculate_volatility_metrics(df)
            if volatility_metrics is None:
                logger.warning(f"{symbol}: 波动性计算失败")
                return None

            # 添加符号标识
            volatility_metrics['symbol'] = symbol

            return volatility_metrics

        except Exception as e:
            logger.error(f"{symbol}: 波动性计算异常: {e}")
            return None

    def _convert_numpy_types(self, obj):
        """递归转换numpy/pandas类型为Python原生类型"""
        if isinstance(obj, dict):
            return {key: self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        else:
            return obj

    async def store_volatility_results(self, volatility_data: Dict):
        """将波动性结果存储到Redis"""
        redis_client = None
        try:
            # 创建临时Redis连接
            from data_processor import CryptoDataProcessor
            temp_processor = CryptoDataProcessor()
            await temp_processor.initialize()
            redis_client = temp_processor.redis_client

            # 存储每个符号的波动性数据
            for symbol, data in volatility_data.items():
                if data is not None:
                    redis_key = self._get_volatility_redis_key(symbol)

                    # 转换numpy类型为Python原生类型
                    converted_data = self._convert_numpy_types(data)

                    # 存储数据（30分钟过期）
                    await redis_client.setex(
                        redis_key, 1800, json.dumps(converted_data)
                    )

            # 更新排行榜
            await self._update_volatility_ranking(volatility_data, redis_client)

            logger.info(f"已将{len([d for d in volatility_data.values() if d is not None])}个符号的波动性数据保存到Redis")

        except Exception as e:
            logger.error(f"存储波动性数据到Redis失败: {e}")
        finally:
            # 清理临时连接
            if redis_client:
                try:
                    await temp_processor.cleanup()
                except Exception:
                    pass

    async def _update_volatility_ranking(self, volatility_data: Dict, redis_client):
        """更新波动性排行榜"""
        try:
            ranking_key = self._get_volatility_ranking_key()

            # 清除旧的排行榜
            await redis_client.delete(ranking_key)

            # 添加新的排行榜数据
            ranking_scores = {}
            for symbol, data in volatility_data.items():
                if data is not None and 'volatility_score' in data:
                    overall_score = data['volatility_score']['overall_score']
                    ranking_scores[symbol] = overall_score

            if ranking_scores:
                # 批量添加到sorted set
                await redis_client.zadd(ranking_key, ranking_scores)

                # 设置过期时间（30分钟）
                await redis_client.expire(ranking_key, 1800)

            logger.debug(f"已更新波动性排行榜，包含{len(ranking_scores)}个符号")

        except Exception as e:
            logger.error(f"更新波动性排行榜失败: {e}")


    async def process_single_symbol(self, symbol: str, fallback_timeframe: str = '1h', fallback_days: int = 30) -> Optional[Dict]:
        """处理单个交易对的波动性分析"""
        async with self.semaphore:
            try:
                # 计算波动性指标
                volatility_data = await self.calculate_volatility_for_symbol(symbol, fallback_timeframe, fallback_days)
                if volatility_data is None:
                    return None

                # 输出关键信息
                self._log_volatility_summary(symbol, volatility_data)

                return volatility_data

            except Exception as e:
                logger.error(f"{symbol}: 处理失败: {e}")
                return None

    def _log_volatility_summary(self, symbol: str, data: Dict):
        """输出波动性摘要信息"""
        try:
            score = data['volatility_score']
            base_info = (f"{symbol}: 波动性 {score['overall_level']}({score['overall_score']:.0f}分) | "
                        f"ATR24h: {data.get('atr_24h_pct', 0):.2f}% | "
                        f"IO24h: {data.get('io_24h', 0):+.2f}%")

            # 如果有换手率数据，添加到输出中
            if 'turnover_24h_raw' in data:
                turnover_info = f" | TO24h: {data.get('turnover_24h_raw', 0):.2f}x"
                base_info += turnover_info

            base_info += f" | 价格: ${data['current_price']:.6f}"
            logger.info(base_info)

        except Exception as e:
            logger.debug(f"{symbol}: 日志输出失败: {e}")

    async def scan_all_symbols(self, fallback_timeframe: str = '1h', fallback_days: int = 30) -> Dict:
        """扫描所有交易对的波动性"""
        start_time = time.time()
        logger.info(f"开始波动性扫描，共{len(self.symbols)}个交易对...")

        # 并发处理所有交易对
        tasks = [self.process_single_symbol(symbol, fallback_timeframe, fallback_days) for symbol in self.symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 整理结果
        volatility_results = {}
        successful_count = 0

        for i, result in enumerate(results):
            symbol = self.symbols[i]

            if isinstance(result, Exception):
                logger.error(f"Error processing {symbol}: {result}")
                volatility_results[symbol] = None
            elif result is not None:
                volatility_results[symbol] = result
                successful_count += 1
            else:
                volatility_results[symbol] = None

        failed_count = len(results) - successful_count
        elapsed_time = time.time() - start_time

        scan_summary = {
            "scan_time": elapsed_time,
            "total_symbols": len(self.symbols),
            "successful_count": successful_count,
            "failed_count": failed_count,
            "timestamp": datetime.now().isoformat(),
            "volatility_data": volatility_results
        }

        logger.info(f"波动性扫描完成: {successful_count}/{len(self.symbols)} 成功, "
                   f"耗时 {elapsed_time:.1f}秒")

        # 存储结果到Redis
        try:
            await self.store_volatility_results(volatility_results)
        except Exception as e:
            logger.warning(f"存储到Redis失败: {e}")

        return scan_summary

    def get_volatility_ranking(self, volatility_data: Dict, limit: int = 10) -> List[Dict]:
        """从扫描结果生成波动性排行榜"""
        try:
            ranking_list = []

            # 过滤有效数据并按波动性评分排序
            valid_data = [(symbol, data) for symbol, data in volatility_data.items()
                         if data is not None and 'volatility_score' in data]

            valid_data.sort(key=lambda x: x[1]['volatility_score']['overall_score'], reverse=True)

            # 生成排行榜
            for i, (symbol, data) in enumerate(valid_data[:limit], 1):
                ranking_item = {
                    'rank': i,
                    'symbol': symbol,
                    'score': data['volatility_score']['overall_score'],
                    'level': data['volatility_score']['overall_level'],
                    'atr_24h_pct': data.get('atr_24h_pct', 0),
                    'io_24h': data.get('io_24h', 0),
                    'current_price': data.get('current_price', 0),
                    'timeframe_minutes': data.get('timeframe_minutes', 0)
                }

                # 如果有换手率数据，添加到排行榜项目中
                if 'turnover_24h_raw' in data:
                    ranking_item['turnover_24h_raw'] = data.get('turnover_24h_raw', 0)

                ranking_list.append(ranking_item)

            return ranking_list

        except Exception as e:
            logger.error(f"生成波动性排行榜失败: {e}")
            return []

    async def get_volatility_ranking_from_redis(self, limit: int = 10) -> List[Dict]:
        """从Redis获取波动性排行榜（备选方法）"""
        redis_client = None
        try:
            # 创建临时Redis连接
            from data_processor import CryptoDataProcessor
            temp_processor = CryptoDataProcessor()
            await temp_processor.initialize()
            redis_client = temp_processor.redis_client

            ranking_key = self._get_volatility_ranking_key()

            # 获取前N个最高波动性的交易对（降序）
            top_symbols = await redis_client.zrevrange(
                ranking_key, 0, limit - 1, withscores=True
            )

            ranking_list = []
            for i, (symbol_bytes, score) in enumerate(top_symbols, 1):
                symbol = symbol_bytes.decode('utf-8') if isinstance(symbol_bytes, bytes) else symbol_bytes

                # 获取详细的波动性数据
                volatility_key = self._get_volatility_redis_key(symbol)
                volatility_data = await redis_client.get(volatility_key)

                if volatility_data:
                    data = json.loads(volatility_data)
                    ranking_item = {
                        'rank': i,
                        'symbol': symbol,
                        'score': score,
                        'level': data['volatility_score']['overall_level'],
                        'atr_24h_pct': data.get('atr_24h_pct', 0),
                        'io_24h': data.get('io_24h', 0),
                        'current_price': data.get('current_price', 0),
                        'timeframe_minutes': data.get('timeframe_minutes', 0)
                    }

                    # 如果有换手率数据，添加到排行榜项目中
                    if 'turnover_24h_raw' in data:
                        ranking_item['turnover_24h_raw'] = data.get('turnover_24h_raw', 0)

                    ranking_list.append(ranking_item)

            return ranking_list

        except Exception as e:
            logger.error(f"从Redis获取波动性排行榜失败: {e}")
            return []
        finally:
            # 清理临时连接
            if redis_client:
                try:
                    await temp_processor.cleanup()
                except Exception:
                    pass

    def display_volatility_report(self, scan_results: Dict, top_count: int = 15):
        """显示波动性报告"""
        try:
            # 从扫描结果生成排行榜
            volatility_data = scan_results.get('volatility_data', {})
            top_volatile = self.get_volatility_ranking(volatility_data, top_count)

            if top_volatile:
                # 显示简化的波动性报告（去掉换手等级和归一化TO）
                logger.info("=" * 80)
                logger.info(f"📊 波动性与换手率排行榜 (TOP {top_count})")
                logger.info("=" * 80)
                logger.info(f"{'排名':<6} {'交易对':<8} {'波动等级':<8} {'ATR24h%':<9} "
                           f"{'IO24h%':<9} {'换手24h':<10} {'当前价格':<14}")
                logger.info("-" * 80)

                for item in top_volatile:
                    turnover_raw = item.get('turnover_24h_raw', 0)

                    # 如果没有换手率数据，显示占位符
                    if turnover_raw == 0 and 'turnover_24h_raw' not in item:
                        turnover_display = "N/A"
                    else:
                        turnover_display = f"{turnover_raw:.2f}x"

                    logger.info(f"{item['rank']:<6} {item['symbol']:<12} {item['level']:<12} "
                               f"{item['atr_24h_pct']:<9.2f} "
                               f"{item['io_24h']:<9.2f} {turnover_display:<10} "
                               f"${item['current_price']:<13.6f}")

                logger.info("-" * 80)

                # 添加换手率统计摘要
                valid_turnovers = [item.get('turnover_24h_raw', 0) for item in top_volatile if item.get('turnover_24h_raw', 0) > 0]
                if valid_turnovers:
                    avg_turnover = sum(valid_turnovers) / len(valid_turnovers)
                    max_turnover = max(valid_turnovers)
                    min_turnover = min(valid_turnovers)

                    logger.info(f"📈 换手率统计: 平均 {avg_turnover:.2f}x | 最高 {max_turnover:.2f}x | 最低 {min_turnover:.2f}x | 有效数据 {len(valid_turnovers)}/{len(top_volatile)}")
                else:
                    logger.info(f"📈 换手率统计: 暂无有效数据 (可能需要持仓量数据)")

                logger.info("=" * 80)
            else:
                logger.warning("暂无波动性数据")

        except Exception as e:
            logger.error(f"显示波动性报告失败: {e}")

    async def run_continuous(self, fallback_timeframe: str = '1h', fallback_days: int = 30):
        """持续运行波动性监控"""
        self.is_running = True
        logger.info(f"波动性监控服务启动，更新间隔: {self.update_interval}秒")
        logger.info(f"优先使用Redis缓存数据，fallback参数: {fallback_timeframe} 时间框架, {fallback_days}天历史数据")

        while self.is_running:
            try:
                # 执行扫描
                scan_result = await self.scan_all_symbols(fallback_timeframe, fallback_days)

                # 显示报告
                self.display_volatility_report(scan_result, self.display_top_count)

                # 等待下次扫描
                if self.is_running:
                    await asyncio.sleep(self.update_interval)

            except KeyboardInterrupt:
                logger.info("收到停止信号，正在关闭...")
                self.is_running = False
                break
            except Exception as e:
                logger.error(f"监控循环出错: {e}")
                await asyncio.sleep(60)

        logger.info("波动性监控服务已停止")

    async def run_single_scan(self, fallback_timeframe: str = '1h', fallback_days: int = 30, top_count: int = 15) -> Dict:
        """执行单次扫描"""
        logger.info("执行单次波动性扫描...")
        logger.info(f"优先使用Redis缓存数据，fallback参数: {fallback_timeframe} 时间框架, {fallback_days}天历史数据")
        scan_result = await self.scan_all_symbols(fallback_timeframe, fallback_days)
        self.display_volatility_report(scan_result, top_count)
        return scan_result

    def stop(self):
        """停止服务"""
        self.is_running = False

async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='TET波动性与换手率监控服务')
    parser.add_argument('--once', action='store_true', help='只执行一次扫描后退出')
    parser.add_argument('--interval', type=int, default=300, help='扫描间隔（秒）')
    parser.add_argument('--top', type=int, default=15, help='显示排行榜数量')
    parser.add_argument('--timeframe', type=str, default='1h', help='数据时间框架 (15m, 30m, 1h, 4h, 1d)')
    parser.add_argument('--days', type=int, default=30, help='历史数据天数')
    parser.add_argument('--example', action='store_true', help='显示换手率计算示例并退出')

    args = parser.parse_args()

    # 如果是显示示例，直接调用示例函数并退出
    if args.example:
        calculate_turnover_rate_example()
        return

    logger.info("启动TET波动性与换手率监控服务...")
    logger.info(f"参数: {args.timeframe} 时间框架, {args.days}天历史数据, 显示TOP{args.top}")

    async with VolatilityMonitor() as monitor:
        if args.interval != 300:
            monitor.update_interval = args.interval

        try:
            if args.once:
                # 单次扫描模式
                result = await monitor.run_single_scan(args.timeframe, args.days, args.top)
                print(f"\n波动性扫描完成:")
                print(f"成功: {result['successful_count']}/{result['total_symbols']}")
                print(f"耗时: {result['scan_time']:.1f}秒")
            else:
                # 持续监控模式
                monitor.display_top_count = args.top
                await monitor.run_continuous(args.timeframe, args.days)

        except KeyboardInterrupt:
            logger.info("用户中断，正在优雅关闭...")

def calculate_turnover_rate_example():
    """
    换手率计算示例函数

    展示如何使用换手率计算功能：
    1. 原始换手率 = Volume / Open Interest
    2. 归一化换手率映射到 [-0.5, 0.5] 区间
    """
    print("\n" + "="*60)
    print("📊 合约换手率计算示例")
    print("="*60)

    # 示例数据
    examples = [
        {"symbol": "BTC/USDT", "volume": 1000000000, "oi": 500000000},  # 2.0x换手
        {"symbol": "ETH/USDT", "volume": 800000000, "oi": 400000000},   # 2.0x换手
        {"symbol": "MEME/USDT", "volume": 500000000, "oi": 100000000},  # 5.0x换手
        {"symbol": "STABLE/USDT", "volume": 100000000, "oi": 200000000}, # 0.5x换手
    ]

    print(f"{'交易对':<12} {'成交量(USD)':<15} {'持仓量(USD)':<15} {'原始换手率':<12} {'归一化换手率':<15} {'换手等级':<10}")
    print("-" * 95)

    for example in examples:
        symbol = example["symbol"]
        volume = example["volume"]
        oi = example["oi"]

        # 计算原始换手率
        raw_rate = volume / oi if oi > 0 else 0

        # 计算归一化换手率 (映射到 [-0.5, 0.5])
        r_log = math.log(1 + abs(raw_rate))
        r_norm = r_log / (1 + r_log)
        normalized_rate = r_norm - 0.5

        # 计算换手等级
        level_info = VolatilityAnalyzer.calculate_turnover_level(raw_rate)
        level = level_info['level']

        print(f"{symbol:<12} {volume:>13,.0f} {oi:>13,.0f} {raw_rate:>10.2f}x "
              f"{normalized_rate:>13.3f} {level:<10}")

    print("="*60)
    print("说明：")
    print("• 原始换手率 = 成交量 ÷ 持仓量")
    print("• 归一化换手率范围：[-0.5, 0.5]，0附近为正常，±0.5为极值")
    print("• 换手等级：MINIMAL < LOW < MEDIUM < HIGH < EXTREME")
    print("• 高换手率通常意味着市场活跃度高，但也可能伴随高波动")
    print("="*60 + "\n")

if __name__ == "__main__":
    import sys

    # 检查是否要运行示例
    if len(sys.argv) > 1 and sys.argv[1] == '--example':
        calculate_turnover_rate_example()
        sys.exit(0)

    asyncio.run(main())