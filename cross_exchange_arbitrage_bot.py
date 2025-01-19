from binance import AsyncClient, BinanceSocketManager
from kucoin import Client as KucoinClient
import asyncio
import websockets
import json
import logging
import sys
from decimal import Decimal
import os
from dotenv import load_dotenv
import time
import multiprocessing
import hmac
import base64
import hashlib
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)

class CrossExchangeArbitrageBot:
    def __init__(self, simulation=False, testnet=True):
        self.simulation = simulation
        self.testnet = testnet
        self.prices = {
            'binance': {},
            'kucoin': {}
        }
        
        # Trading pairs
        self.binance_pair = 'TRUMPUSDC'
        self.kucoin_pair = 'TRUMP-USDT'  # KuCoin uses different pair format
        
        # Initialize Decimal cache
        self.decimal_cache = {
            str(i/10): Decimal(str(i/10)) for i in range(10, 1000)
        }
        
        # Constants
        self.DECIMAL_100 = Decimal('100')
        self.DECIMAL_0 = Decimal('0')
        self.position_size = Decimal('20.0')
        self.min_profit_threshold_decimal = Decimal('1.0')  # Increased for cross-exchange
        self.max_slippage_decimal = Decimal('0.3')
        self.trade_cooldown = 15  # Increased for cross-exchange
        self.last_trade_time = 0
        
        # Socket flags
        self.binance_socket_started = False
        self.kucoin_socket_started = False
        self.ready_to_trade = asyncio.Event()
        self.kucoin_ws_url = None
        self.kucoin_ws_token = None

        # Processor setup
        cpu_count = multiprocessing.cpu_count()
        self.num_processors = min(max(2, int(cpu_count * 0.75)), 8)
        self.message_queue = asyncio.Queue(maxsize=self.num_processors * 200)
        self.processor_tasks = []

    def get_decimal_price(self, price_str):
        """Get cached Decimal price or create new one if not in cache"""
        return self.decimal_cache.get(str(price_str)) or Decimal(str(price_str))

    async def initialize(self):
        """Initialize exchange clients and socket managers"""
        load_dotenv()
        
        # Binance setup
        prefix = "TESTNET_" if self.testnet else ""
        binance_api_key = os.getenv(f'{prefix}BINANCE_API_KEY')
        binance_api_secret = os.getenv(f'{prefix}BINANCE_API_SECRET')
        
        # KuCoin setup
        self.kucoin_api_key = os.getenv('KUCOIN_API_KEY')
        self.kucoin_api_secret = os.getenv('KUCOIN_API_SECRET')
        self.kucoin_api_passphrase = os.getenv('KUCOIN_API_PASSPHRASE')
        
        if not all([binance_api_key, binance_api_secret, self.kucoin_api_key, 
                   self.kucoin_api_secret, self.kucoin_api_passphrase]):
            raise ValueError("Missing API credentials")
            
        # Initialize Binance
        self.binance_client = await AsyncClient.create(
            binance_api_key,
            binance_api_secret,
            testnet=self.testnet
        )
        self.binance_socket = BinanceSocketManager(self.binance_client, user_timeout=60)
        
        # Initialize KuCoin
        self.kucoin_client = KucoinClient(
            self.kucoin_api_key,
            self.kucoin_api_secret,
            self.kucoin_api_passphrase,
            sandbox=self.testnet
        )
        
        # Get WebSocket details
        ws_details = self.kucoin_client.get_ws_endpoint()
        self.kucoin_ws_token = ws_details['token']
        self.kucoin_ws_url = ws_details['instanceServers'][0]['endpoint']
        
        # Get trading precision info
        binance_info = await self.binance_client.get_symbol_info(self.binance_pair)
        self.binance_step_size = Decimal(next(
            f['stepSize'] for f in binance_info['filters'] 
            if f['filterType'] == 'LOT_SIZE'
        ))
        
        # Get KuCoin trading precision
        kucoin_info = self.kucoin_client.get_symbols()
        self.kucoin_step_size = Decimal(next(
            s['baseIncrement'] for s in kucoin_info 
            if s['symbol'] == self.kucoin_pair
        ))

    async def binance_socket_manager(self):
        """Manage Binance websocket connection"""
        while True:
            try:
                streams = [f"{self.binance_pair.lower()}@ticker"]
                socket = self.binance_socket.multiplex_socket(streams)
                
                async with socket as ts:
                    while True:
                        try:
                            msg = await ts.recv()
                            if msg:
                                await self.message_queue.put(('binance', msg))
                        except Exception as e:
                            logging.error(f"Error receiving Binance message: {e}")
                            break
                            
            except Exception as e:
                logging.error(f"Binance socket error: {e}")
                await asyncio.sleep(5)
                
    async def message_processor(self, processor_id):
        """Process messages from both exchanges"""
        while True:
            try:
                exchange, msg = await self.message_queue.get()
                if exchange == 'binance':
                    await self.process_binance_message(msg)
                elif exchange == 'kucoin':
                    await self.process_kucoin_message(msg)
                self.message_queue.task_done()
            except Exception as e:
                logging.error(f"Processor {processor_id} error: {e}")
                await asyncio.sleep(0.1)

    async def process_binance_message(self, msg):
        """Process Binance ticker message"""
        try:
            # Check if it's a ticker message
            if 'stream' not in msg or 'data' not in msg:
                return

            data = msg['data']
            symbol = data.get('s')
            
            if not symbol or symbol != self.binance_pair:
                return

            # Extract prices from Binance message
            bid_price = self.get_decimal_price(data.get('b', '0'))  # Best bid
            ask_price = self.get_decimal_price(data.get('a', '0'))  # Best ask
            last_price = self.get_decimal_price(data.get('c', '0'))  # Last price
            timestamp = data.get('E', int(time.time() * 1000))
            
            self.prices['binance'][symbol] = {
                'bid': bid_price,
                'ask': ask_price,
                'last_price': last_price,
                'update_time': timestamp
            }
            
            #logging.info(f"Binance {symbol} - Bid: {bid_price}, Ask: {ask_price}, Last: {last_price}")
            
            if not self.binance_socket_started:
                self.binance_socket_started = True
                self.check_ready_to_trade()
                
            # Check for arbitrage opportunities
            await self.check_arbitrage()
                
        except Exception as e:
            logging.error(f"Error processing Binance message: {e}")
            logging.debug(f"Problematic message: {msg}")

    async def kucoin_socket_manager(self):
        """Manage KuCoin WebSocket connection"""
        while True:
            try:
                # Connect to KuCoin WebSocket
                async with websockets.connect(f"{self.kucoin_ws_url}?token={self.kucoin_ws_token}") as ws:
                    # Subscribe to ticker
                    subscribe_message = {
                        "id": str(uuid.uuid4()),
                        "type": "subscribe",
                        "topic": f"/market/ticker:{self.kucoin_pair}",
                        "privateChannel": False,
                        "response": True
                    }
                    
                    await ws.send(json.dumps(subscribe_message))
                    
                    # Process messages
                    while True:
                        try:
                            message = await ws.recv()
                            if message:
                                msg = json.loads(message)
                                if msg.get('type') == 'message':
                                    await self.message_queue.put(('kucoin', msg))
                        except websockets.ConnectionClosed:
                            break
                            
            except Exception as e:
                logging.error(f"KuCoin socket error: {e}")
                await asyncio.sleep(5)
                # Refresh token if needed
                try:
                    ws_details = self.kucoin_client.get_ws_token()
                    self.kucoin_ws_token = ws_details['token']
                except Exception as token_error:
                    logging.error(f"Error refreshing KuCoin token: {token_error}")
                    await asyncio.sleep(5)

    async def process_kucoin_message(self, msg):
        """Process KuCoin ticker message"""
        try:
            # Check if it's a proper ticker message
            if not (msg.get('type') == 'message' and 
                    msg.get('subject') == 'trade.ticker' and 
                    msg.get('topic', '').startswith('/market/ticker:')):
                return

            data = msg.get('data')
            if not data:
                return

            # Extract symbol from topic
            topic = msg.get('topic', '')
            symbol = topic.split(':')[1] if ':' in topic else None
            
            if not symbol or symbol != self.kucoin_pair:
                return

            # Extract prices from KuCoin message
            bid_price = self.get_decimal_price(data.get('bestBid', '0'))
            ask_price = self.get_decimal_price(data.get('bestAsk', '0'))
            last_price = self.get_decimal_price(data.get('price', '0'))
            timestamp = data.get('time', int(time.time() * 1000))
            
            self.prices['kucoin'][symbol] = {
                'bid': bid_price,
                'ask': ask_price,
                'last_price': last_price,
                'update_time': timestamp
            }
            
            #logging.info(f"KuCoin {symbol} - Bid: {bid_price}, Ask: {ask_price}, Last: {last_price}")
            
            if not self.kucoin_socket_started:
                self.kucoin_socket_started = True
                self.check_ready_to_trade()
                
            # Check for arbitrage opportunities
            await self.check_arbitrage()
                
        except Exception as e:
            logging.error(f"Error processing KuCoin message: {e}")
            logging.debug(f"Problematic message: {msg}")

    async def execute_kucoin_order(self, side, quantity=None):
        """Execute KuCoin order with sign"""
        now = int(time.time() * 1000)
        str_to_sign = str(now) + side + self.kucoin_pair
        
        signature = base64.b64encode(
            hmac.new(
                self.kucoin_api_secret.encode('utf-8'),
                str_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode()
        
        passphrase = base64.b64encode(
            hmac.new(
                self.kucoin_api_secret.encode('utf-8'),
                self.kucoin_api_passphrase.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode()
        
        headers = {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": str(now),
            "KC-API-KEY": self.kucoin_api_key,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2"
        }
        
        order_data = {
            "clientOid": str(uuid.uuid4()),
            "side": side.lower(),
            "symbol": self.kucoin_pair,
            "type": "market"
        }
        
        if side.lower() == 'buy':
            order_data["funds"] = str(self.position_size)
        else:
            order_data["size"] = str(quantity)
            
        return self.kucoin_client.create_order(order_data, headers)

    def check_ready_to_trade(self):
        """Check if both exchanges are ready for trading"""
        if self.binance_socket_started and self.kucoin_socket_started:
            if not self.ready_to_trade.is_set():
                logging.info("Both exchanges connected and ready to trade")
                self.ready_to_trade.set()

    async def check_arbitrage(self):
        """Check for cross-exchange arbitrage opportunities"""
        try:
            if time.time() - self.last_trade_time < self.trade_cooldown:
                return
                
            binance_data = self.prices['binance'].get(self.binance_pair)
            kucoin_data = self.prices['kucoin'].get(self.kucoin_pair)
            
            if not (binance_data and kucoin_data):
                return
                
            # Calculate potential profits
            buy_binance_sell_kucoin = (
                kucoin_data['bid'] - binance_data['ask']
            ) / binance_data['ask'] * self.DECIMAL_100
            
            buy_kucoin_sell_binance = (
                binance_data['bid'] - kucoin_data['ask']
            ) / kucoin_data['ask'] * self.DECIMAL_100
            
            logging.info(f"Binance->KuCoin spread: {buy_binance_sell_kucoin:.2f}% | KuCoin->Binance spread: {buy_kucoin_sell_binance:.2f}%")
            
            if self.simulation:
                if buy_binance_sell_kucoin > self.min_profit_threshold_decimal:
                    logging.info(f"SIMULATION: Would execute Binance->KuCoin arbitrage with {buy_binance_sell_kucoin:.2f}% profit")
                elif buy_kucoin_sell_binance > self.min_profit_threshold_decimal:
                    logging.info(f"SIMULATION: Would execute KuCoin->Binance arbitrage with {buy_kucoin_sell_binance:.2f}% profit")
                return
                
            if not await self.check_balances():
                return
                
            # Execute trades if profitable
            if buy_binance_sell_kucoin > self.min_profit_threshold_decimal:
                quantity = await self.calculate_trade_amount(
                    price=binance_data['ask'],
                    side='buy',
                    exchange='binance'
                )
                if quantity:
                    await self.execute_cross_exchange_arbitrage(
                        buy_exchange='binance',
                        sell_exchange='kucoin',
                        buy_price=binance_data['ask'],
                        sell_price=kucoin_data['bid'],
                        quantity=quantity
                    )
                
            elif buy_kucoin_sell_binance > self.min_profit_threshold_decimal:
                quantity = await self.calculate_trade_amount(
                    price=kucoin_data['ask'],
                    side='buy',
                    exchange='kucoin'
                )
                if quantity:
                    await self.execute_cross_exchange_arbitrage(
                        buy_exchange='kucoin',
                        sell_exchange='binance',
                        buy_price=kucoin_data['ask'],
                        sell_price=binance_data['bid'],
                        quantity=quantity
                    )
                    
        except Exception as e:
            logging.error(f"Error in check_arbitrage: {e}")

    async def calculate_trade_amount(self, price, side, exchange):
        """Calculate the amount of TRUMP to trade based on position size and exchange rules"""
        try:
            # Calculate base amount from position size
            amount = self.position_size / price
            
            # Get step size for the relevant exchange
            step_size = self.binance_step_size if exchange == 'binance' else self.kucoin_step_size
            
            # Normalize amount according to step size
            decimal_places = abs(Decimal(step_size).as_tuple().exponent)
            normalized_amount = (amount // step_size) * step_size
            normalized_amount = normalized_amount.quantize(Decimal('0.' + '0' * decimal_places))
            
            # Additional exchange-specific checks
            if exchange == 'binance':
                # Get symbol info for minimum notional and lot size
                symbol_info = await self.binance_client.get_symbol_info(self.binance_pair)
                min_notional = next(
                    float(f['minNotional']) 
                    for f in symbol_info['filters'] 
                    if f['filterType'] == 'MIN_NOTIONAL'
                )
                
                # Check if trade meets minimum notional
                if normalized_amount * price < min_notional:
                    logging.warning(f"Trade amount {normalized_amount} below minimum notional for Binance")
                    return None
                    
            elif exchange == 'kucoin':
                # Get KuCoin trading rules
                symbol_details = next(
                    s for s in self.kucoin_client.get_symbols() 
                    if s['symbol'] == self.kucoin_pair
                )
                min_size = Decimal(symbol_details['minSize'])
                
                # Check if trade meets minimum size
                if normalized_amount < min_size:
                    logging.warning(f"Trade amount {normalized_amount} below minimum size for KuCoin")
                    return None
            
            return normalized_amount
            
        except Exception as e:
            logging.error(f"Error calculating trade amount: {e}")
            return None

    async def execute_cross_exchange_arbitrage(self, buy_exchange, sell_exchange, buy_price, sell_price, quantity):
        """Execute arbitrage trades across exchanges"""
        try:
            # Verify current prices
            current_prices = self.prices[buy_exchange]
            exchange_data = current_prices.get(
                self.binance_pair if buy_exchange == 'binance' else self.kucoin_pair
            )
            
            if not exchange_data:
                return
                
            price_movement = abs(Decimal(exchange_data['ask']) - buy_price) / buy_price * self.DECIMAL_100
            if price_movement > self.max_slippage_decimal:
                logging.warning(f"Price moved too much: {price_movement}%")
                return
                
            # Execute buy order
            if buy_exchange == 'binance':
                buy_order = await self.binance_client.create_order(
                    symbol=self.binance_pair,
                    side='BUY',
                    type='MARKET',
                    quantity=quantity
                )
                
                # Small delay for order processing
                await asyncio.sleep(0.2)
                
                # Verify TRUMP balance on Binance
                trump_balance = Decimal((await self.binance_client.get_asset_balance(asset='TRUMP'))['free'])
                if trump_balance < quantity:
                    logging.error("Failed to receive TRUMP tokens on Binance")
                    return
                    
                # Withdraw from Binance to KuCoin
                deposit_address = self.kucoin_client.get_deposit_address('TRUMP')
                await self.binance_client.withdraw(
                    asset='TRUMP',
                    address=deposit_address['address'],
                    amount=quantity
                )
                
                # Wait for confirmation and sell on KuCoin
                # You might want to implement a more sophisticated way to wait for the deposit
                await asyncio.sleep(30)  # Adjust based on typical transfer time
                
                # Sell on KuCoin
                sell_order = await self.execute_kucoin_order('SELL', quantity)
                
            else:  # buy_exchange == 'kucoin'
                buy_order = await self.execute_kucoin_order('BUY')
                
                # Small delay for order processing
                await asyncio.sleep(0.2)
                
                # Verify TRUMP balance on KuCoin
                kucoin_accounts = self.kucoin_client.get_accounts()
                trump_balance = Decimal(next(
                    acc['available'] for acc in kucoin_accounts 
                    if acc['currency'] == 'TRUMP' and acc['type'] == 'trade'
                ))
                if trump_balance < quantity:
                    logging.error("Failed to receive TRUMP tokens on KuCoin")
                    return
                    
                # Withdraw from KuCoin to Binance
                binance_deposit_info = await self.binance_client.get_deposit_address(coin='TRUMP')
                withdrawal = self.kucoin_client.create_withdrawal('TRUMP', quantity, binance_deposit_info['address'])
                
                # Wait for confirmation and sell on Binance
                await asyncio.sleep(30)  # Adjust based on typical transfer time
                
                sell_order = await self.binance_client.create_order(
                    symbol=self.binance_pair,
                    side='SELL',
                    type='MARKET',
                    quantity=quantity
                )
            
            if buy_order and sell_order:
                profit = (sell_price - buy_price) * quantity
                logging.info(f"Cross-exchange arbitrage completed. Profit: {profit} USDT")
                self.last_trade_time = time.time()
                
        except Exception as e:
            logging.error(f"Error executing cross-exchange arbitrage: {e}")

    async def start(self):
        """Start the arbitrage bot"""
        await self.initialize()
        logging.info(f"Starting {'simulation' if self.simulation else 'live'} cross-exchange arbitrage bot...")
        
        try:
            # Start socket managers
            binance_socket_task = asyncio.create_task(self.binance_socket_manager())
            kucoin_socket_task = asyncio.create_task(self.kucoin_socket_manager())
            
            # Start message processors
            self.processor_tasks = [
                asyncio.create_task(self.message_processor(i))
                for i in range(self.num_processors)
            ]
            
            # Wait for sockets and processors
            await asyncio.gather(
                binance_socket_task,
                kucoin_socket_task,
                self.ready_to_trade.wait(),
                *self.processor_tasks,
                return_exceptions=True
            )
            
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
        finally:
            # Cleanup
            await self.close()

    async def close(self):
        """Cleanup resources"""
        for task in self.processor_tasks:
            task.cancel()
        await asyncio.gather(*self.processor_tasks, return_exceptions=True)
        await self.binance_client.close_connection()

async def main():
    bot = CrossExchangeArbitrageBot(simulation=True, testnet=False)
    try:
        await bot.start()
    except KeyboardInterrupt:
        logging.info("Shutting down bot...")
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())