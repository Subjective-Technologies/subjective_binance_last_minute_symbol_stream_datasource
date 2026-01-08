import time
import json
import os
from datetime import datetime, timedelta
from subjective_abstract_data_source_package import SubjectiveRealTimeDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveLastMinuteSymbolStreamDataSource(SubjectiveRealTimeDataSource):
    connection_type = "ExchangeStream"
    connection_fields = ["exchange", "api_key", "api_secret", "minutes_ago", "symbols"]

    def __init__(self, name=None, session=None, dependency_data_sources=[], subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources,
            subscribers=subscribers,
            params=params
        )
        self.symbol_data_buffer = {}  # Buffer to store ticker updates by symbol
        self.target_symbols = []  # List of symbols to monitor
        self.minutes_ago = 1
        self.last_output_time = datetime.now()
        self.output_file_path = None  # Will hold the path to our single output file
        self.candle_data = []  # Array to hold all candle data
        self._twm = None
        self._api_key = ""
        self._api_secret = ""
        self._symbols_param = ""

    def get_icon(self):
        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        try:
            with open(icon_path, 'r') as f:
                return f.read()
        except Exception as e:
            BBLogger.log(f"Error reading icon file: {str(e)}")
            return ""

    def get_connection_data(self):
        return {"connection_type": self.connection_type, "fields": list(self.connection_fields)}

    def notify(self, data):
        self.update(data)

    def _get_param(self, key, default=None):
        return self.params.get(key, default)

    def _initialize_output_file(self):
        """Initialize the single output file with timestamp in filename"""
        if self.output_file_path is not None:
            return  # Already initialized

        target_dir = self._get_param("TARGET_DIRECTORY", ".")
        os.makedirs(target_dir, exist_ok=True)

        # Create filename with timestamp at initialization
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        datasource_name = self.get_data_source_type_name()
        filename = f"{timestamp}-{datasource_name}-context.json"
        self.output_file_path = os.path.join(target_dir, filename)

        # Initialize file with empty array
        with open(self.output_file_path, 'w') as f:
            json.dump([], f)

        BBLogger.log(f"Initialized output file: {self.output_file_path}")

    def _append_to_output_file(self, candle_data):
        """Append new candle data to the output file"""
        try:
            # Read existing data
            with open(self.output_file_path, 'r') as f:
                existing_data = json.load(f)

            # Append new candle
            existing_data.append(candle_data)

            # Write back
            with open(self.output_file_path, 'w') as f:
                json.dump(existing_data, f, indent=2, default=str)

            BBLogger.log(f"Appended candle data to {self.output_file_path} (total candles: {len(existing_data)})")
        except Exception as e:
            BBLogger.log(f"Error appending to file: {str(e)}")

    def _on_tick(self, msg):
        """Callback for websocket ticker updates"""
        try:
            # Handle the websocket message format
            if isinstance(msg, dict):
                # Check if this is the data array (multiple symbols)
                if 'data' in msg and isinstance(msg['data'], list):
                    tickers = msg['data']
                    for ticker_data in tickers:
                        self._process_ticker_data(ticker_data)
                # Single ticker update
                elif 's' in msg:  # 's' is the symbol field in Binance ticker
                    self._process_ticker_data(msg)
        except Exception as e:
            BBLogger.log(f"Error processing tick: {str(e)}")

    def _process_ticker_data(self, ticker_data):
        """Process individual ticker data"""
        try:
            symbol = ticker_data.get('s', ticker_data.get('symbol', 'UNKNOWN'))

            # Filter by target symbols if specified
            if self.target_symbols and symbol not in self.target_symbols:
                return

            # Extract relevant data
            processed_data = {
                'symbol': symbol,
                'price': ticker_data.get('c', ticker_data.get('lastPrice', '0')),
                'volume': ticker_data.get('v', ticker_data.get('volume', '0')),
                'timestamp': datetime.now().isoformat(),
                'high': ticker_data.get('h', ticker_data.get('highPrice', '0')),
                'low': ticker_data.get('l', ticker_data.get('lowPrice', '0')),
                'open': ticker_data.get('o', ticker_data.get('openPrice', '0')),
                'priceChange': ticker_data.get('p', ticker_data.get('priceChange', '0')),
                'priceChangePercent': ticker_data.get('P', ticker_data.get('priceChangePercent', '0'))
            }

            # Store in buffer
            self.symbol_data_buffer[symbol] = processed_data

            # Check if it's time to output (every N minutes)
            current_time = datetime.now()
            if (current_time - self.last_output_time).total_seconds() >= (self.minutes_ago * 60):
                self._output_buffered_data()
                self.last_output_time = current_time

        except Exception as e:
            BBLogger.log(f"Error processing ticker data: {str(e)}")

    def _output_buffered_data(self):
        """Output all buffered symbol data"""
        if not self.symbol_data_buffer:
            return

        candle_data = {
            'timestamp': datetime.now().isoformat(),
            'symbol_count': len(self.symbol_data_buffer),
            'symbols': list(self.symbol_data_buffer.values())
        }

        # Append to our single output file
        self._append_to_output_file(candle_data)

        BBLogger.log(f"Output data for {len(self.symbol_data_buffer)} symbols")

        # Clear buffer after output
        self.symbol_data_buffer.clear()

    def update(self, data):
        """Override base class update to prevent duplicate file creation"""
        # We handle file writing ourselves via _append_to_output_file
        # So we skip the base class's _write_context_output call
        # But we still notify subscribers
        self.increment_processed_items()
        self._emit_progress()
        for subscriber in self.subscribers:
            subscriber.notify(data)

    def _initialize_monitoring(self):
        """Prepare monitoring configuration before starting the websocket stream."""
        # Initialize the output file at start
        self._initialize_output_file()

        self.minutes_ago = int(self._get_param("minutes_ago", 1))
        self._api_key = self._get_param("api_key", "")
        self._api_secret = self._get_param("api_secret", "")
        self._symbols_param = self._get_param("symbols", "BTCUSDT")

        BBLogger.log(f"Starting Last {self.minutes_ago} minutes symbol stream for Binance")
        BBLogger.log(f"API Key: {self._api_key[:10]}...")
        BBLogger.log(f"Symbols: {self._symbols_param}")

        # Parse symbols
        if isinstance(self._symbols_param, str):
            if self._symbols_param.upper() == "ALL" or self._symbols_param == "":
                self.target_symbols = []  # Empty means all symbols
            else:
                self.target_symbols = [s.strip().upper() for s in self._symbols_param.split(',')]
        else:
            self.target_symbols = []

    def _connect_stream(self):
        from binance import ThreadedWebsocketManager

        self._twm = ThreadedWebsocketManager(api_key=self._api_key, api_secret=self._api_secret)
        self._twm.start()
        BBLogger.log("ThreadedWebsocketManager started successfully")

    def _run_stream(self):
        if not self._twm:
            raise RuntimeError("ThreadedWebsocketManager not initialized")

        # Subscribe to all symbols ticker stream
        # !ticker@arr provides all market tickers in array format
        self._twm.start_multiplex_socket(callback=self._on_tick, streams=['!ticker@arr'])

        BBLogger.log("Subscribed to Binance ticker stream: !ticker@arr")
        BBLogger.log("Websocket stream is now active and receiving data...")
        self._twm.join()

    def _disconnect_stream(self):
        if self._twm:
            try:
                self._twm.stop()
            finally:
                self._twm = None

    def _start_monitoring_implementation(self):
        """Start the websocket stream with automatic reconnection."""
        self._run_with_reconnect(
            connect_fn=self._connect_stream,
            run_fn=self._run_stream,
            disconnect_fn=self._disconnect_stream,
            label="Binance ticker stream"
        )

    def fetch(self):
        if self.status_callback:
            self.status_callback(self.get_name(), "stream_started")
        self.start_monitoring()
        BBLogger.log(f"Stream started for {self.get_name()}")
