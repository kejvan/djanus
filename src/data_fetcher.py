# data_fetcher.py
# Minimal data fetcher for Djanus investment strategy system
#
# Provides DataFetcher class for retrieving financial data from Yahoo Finance
# Handles single tickers with unified interface
# Returns raw Polars DataFrames for processing by DataProcessor
# Supports optional saving to CSV/Parquet files
# Can load existing files if they match parameters
#
# Main methods:
# - get_data(): retrieve raw market data
# - load_data(): load existing data from file
# - save_data(): persist DataFrame to file
# - fetch_data(): complete pipeline (check existing -> load or download -> optional save)

from typing import Union, Optional
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import polars as pl
import yfinance as yf
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type aliases
DateLike = Union[str, datetime]


class TimeInterval(Enum):
    """Supported time intervals for financial data."""

    DAILY = "1d"
    WEEKLY = "1wk"
    MONTHLY = "1mo"
    QUARTERLY = "3mo"


class FileFormat(Enum):
    """Supported file formats for data storage."""

    CSV = "csv"
    PARQUET = "parquet"


class DataFetcher:
    """Minimal data fetcher for Yahoo Finance data using yfinance."""

    # Helper methods
    def _parse_date(self, date: Optional[DateLike], days_back: int = 365) -> str:
        """
        Convert date to YYYY-MM-DD string format.

        Args:
            date: Date as string 'YYYY-MM-DD', datetime object, or None
            days_back: Days back from now for None dates (0 for current time)

        Returns:
            Date string in YYYY-MM-DD format
        """
        if date is None:
            base_date = (
                datetime.now()
                if days_back == 0
                else datetime.now() - timedelta(days=days_back)
            )
            return base_date.strftime("%Y-%m-%d")
        elif isinstance(date, str):
            try:
                datetime.strptime(date, "%Y-%m-%d")
                return date
            except ValueError:
                raise ValueError(f"Invalid date format '{date}'. Expected YYYY-MM-DD")
        else:
            return date.strftime("%Y-%m-%d")

    # Main methods
    def get_data(
        self,
        ticker: str = "^GSPC",
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        interval: TimeInterval = TimeInterval.DAILY,
    ) -> pl.DataFrame:
        """
        Fetch historical data for a single ticker.

        Args:
            ticker: Ticker symbol (e.g., 'SPY', '^GSPC')
            start_date: Start date as string 'YYYY-MM-DD' or datetime object
            end_date: End date as string 'YYYY-MM-DD' or datetime object
            interval: Data interval from TimeInterval enum

        Returns:
            Polars DataFrame with raw OHLCV data and interval metadata
        """
        start_str = self._parse_date(start_date, 365)
        end_str = self._parse_date(end_date, 0)

        logger.info(f"Fetching {ticker} from {start_str} to {end_str}")

        try:
            ticker_obj = yf.Ticker(ticker)
            data = ticker_obj.history(
                start=start_str, end=end_str, interval=interval.value
            )
        except Exception as e:
            raise ValueError(f"Failed to fetch data for {ticker}: {e}")

        if data.empty:
            raise ValueError(f"No data found for {ticker}")

        # Convert to Polars and add metadata
        df = pl.from_pandas(data.reset_index())
        df = df.with_columns(
            [
                pl.col("Date").cast(pl.Date),
                pl.lit(ticker).alias("Ticker"),
                pl.lit(interval.value).alias("Interval"),
            ]
        )

        logger.info(f"Retrieved {len(df)} rows")
        return df

    def load_data(self, filepath: Union[str, Path]) -> pl.DataFrame:
        """
        Load existing data from file.

        Args:
            filepath: Path to the data file

        Returns:
            Polars DataFrame with raw data
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        if filepath.suffix == ".csv":
            df = pl.read_csv(filepath)
        elif filepath.suffix == ".parquet":
            df = pl.read_parquet(filepath)
        else:
            raise ValueError(f"Unsupported format: {filepath.suffix}")

        # Ensure Date is proper type
        if "Date" in df.columns:
            df = df.with_columns(pl.col("Date").cast(pl.Date))

        return df

    def save_data(
        self,
        df: pl.DataFrame,
        filepath: Union[str, Path],
        format: FileFormat = FileFormat.CSV,
    ) -> None:
        """
        Save DataFrame to file.

        Args:
            df: Polars DataFrame to save
            filepath: Path where to save the file
            format: File format from FileFormat enum
        """
        if df.is_empty():
            raise ValueError("Cannot save empty DataFrame")

        filepath = Path(filepath)

        # Ensure extension matches format
        expected_ext = f".{format.value}"
        if filepath.suffix != expected_ext:
            filepath = filepath.with_suffix(expected_ext)

        # Create directory only when actually saving
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if format == FileFormat.CSV:
            df.write_csv(filepath)
        elif format == FileFormat.PARQUET:
            df.write_parquet(filepath)
        else:
            raise ValueError(f"Unsupported format: {format}")

        logger.info(f"Saved to {filepath}")

    def fetch_data(
        self,
        ticker: str = "^GSPC",
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        interval: TimeInterval = TimeInterval.DAILY,
        save: bool = False,
        filepath: Optional[Union[str, Path]] = None,
        format: FileFormat = FileFormat.CSV,
    ) -> pl.DataFrame:
        """
        Complete pipeline to fetch raw financial data - checks for existing file first.

        Args:
            ticker: Ticker symbol (e.g., 'SPY', '^GSPC')
            start_date: Start date as string 'YYYY-MM-DD' or datetime object
            end_date: End date as string 'YYYY-MM-DD' or datetime object
            interval: Data interval from TimeInterval enum
            save: Whether to save data to file
            filepath: Path where to save/load the file (auto-generated if None)
            format: File format from FileFormat enum

        Returns:
            Raw Polars DataFrame with OHLCV data
        """

        # Generate filepath if saving and none provided
        if save and filepath is None:
            start_str = self._parse_date(start_date, 365).replace("-", "")
            end_str = self._parse_date(end_date, 0).replace("-", "")
            filepath = f"data/raw/{ticker}_{start_str}_{end_str}_{interval.value}.{format.value}"

        # Try loading existing file first
        if filepath and Path(filepath).exists():
            try:
                df = self.load_data(filepath)
                logger.info(f"Loaded existing data from {filepath}")
                return df
            except Exception as e:
                logger.warning(f"Failed to load {filepath}: {e}")

        # Fetch fresh data
        df = self.get_data(ticker, start_date, end_date, interval)

        # Save if requested
        if save and filepath:
            self.save_data(df, filepath, format)

        return df


# Simple test/demo when running this file directly
if __name__ == "__main__":
    print("Testing DataFetcher:")
    fetcher = DataFetcher()
    df = fetcher.fetch_data(ticker="^GSPC", interval=TimeInterval.MONTHLY, save=False)
    print(f"Fetched {len(df)} rows for S&P 500. Latest 3:")
    print(df.tail(3))
