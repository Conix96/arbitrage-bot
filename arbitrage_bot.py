from binance import AsyncClient, BinanceSocketManager
import asyncio
import logging
from decimal import Decimal
import os
import sys
from dotenv import load_dotenv
import time
import multiprocessing

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)

class HybridArbitrageBot:
    def __init__(self, simulation=False, testnet=True):
        self.simulation = simulation
        self.testnet = testnet
        self.prices = {}
        
        # Trading pairs
        self.pair1 = 'TRUMPUSDT'
        self.pair2 = 'TRUMPUSDC'

        # Initialize Decimal cache for common price points (e.g., $0.10 to $100.00)
        self.decimal_cache = {
            str(i/10): Decimal(str(i/10)) for i in range(10, 1000)  # Adjust range based on expected price range
        }
        
        # Commonly used Decimal constants
        self.DECIMAL_100 = Decimal('100')
        self.DECIMAL_0 = Decimal('0')
        
        # Fixed position sizes (in USDT/USDC)
        self.position_size = Decimal('20.0')
        
        # Pre-calculated threshold values as Decimals
        self.min_profit_threshold_decimal = Decimal('0.5')  # 0.5% minimum profit
        self.max_slippage_decimal = Decimal('0.3')  # 0.3% maximum allowed slippage
             
        # Trade cooldown (in seconds)
        self.trade_cooldown = 10
        self.last_trade_time = 0
        
        # Socket update flags
        self.socket_started = False
        self.ready_to_trade = asyncio.Event()

        # Get the CPU count
        cpu_count = multiprocessing.cpu_count()

        #Calculate optimal processors
        # Use 75% of available cores, minimum 2, maximum 8
        self.num_processors = min(max(2, int(cpu_count * 0.75)), 8)
        logging.info(f"{self.num_processors} processors will be used")
        # Adjust queue size based on processors
        self.message_queue = asyncio.Queue(maxsize=self.num_processors * 200)
        self.processor_tasks = []

    def get_env_prefix(self):
        return "TESTNET_" if self.testnet else ""
    
    def get_decimal_price(self, price_str):
        """Get cached Decimal price or create new one if not in cache"""
        return self.decimal_cache.get(price_str) or Decimal(str(price_str))

    async def initialize(self):
        """Initialize Binance clients and socket manager"""
        load_dotenv()
        prefix = self.get_env_prefix()
        api_key = os.getenv(f'{prefix}BINANCE_API_KEY')
        api_secret = os.getenv(f'{prefix}BINANCE_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError(f"Missing {'testnet' if self.testnet else 'mainnet'} API credentials")
            
        self.client = await AsyncClient.create(
            api_key,
            api_secret,
            testnet=self.testnet
        )
        self.bm = BinanceSocketManager(self.client, user_timeout=60)
        
        # Get symbol info for precision
        info = await self.client.get_symbol_info(self.pair1)
        lot_size_filter = next((f for f in info['filters'] if f['filterType'] == 'LOT_SIZE'), None)
        self.step_size = Decimal(lot_size_filter['stepSize'])

    async def message_processor(self, processor_id):
        """Dedicated task for processing messages from the queue"""
        while True:
            try:
                msg = await self.message_queue.get()
                await self.process_socket_message(msg)
                self.message_queue.task_done()
            except Exception as e:
                logging.error(f"Processor {processor_id} error: {e}")
                await asyncio.sleep(0.1)  # Brief pause on error
    
    async def socket_manager(self):
        """Enhanced socket manager with better error handling and message buffering"""
        # Start multiple message processors
        self.processor_tasks = [
            asyncio.create_task(self.message_processor(i))
            for i in range(self.num_processors)
        ]
        while True:
            try:
                streams = [
                    f"{self.pair1.lower()}@ticker",  # Changed from @trade to @ticker
                    f"{self.pair2.lower()}@ticker"   # Changed from @trade to @ticker
                ]
                socket = self.bm.multiplex_socket(streams)
                
                async with socket as ts:
                    while True:
                        try:
                            msg = await asyncio.wait_for(ts.recv(), timeout=30)
                            
                            if msg:
                                try:
                                    await self.message_queue.put(msg)
                                except asyncio.QueueFull:
                                    logging.warning("Internal queue full, skipping message")
                                    continue
                                    
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logging.error(f"Socket error: {e}")
                            await asyncio.sleep(1)
                            break
                                
                # Add delay before reconnecting
                await asyncio.sleep(1)
                
            except Exception as e:
                logging.error(f"Socket manager error: {e}")
                await asyncio.sleep(5)
                logging.info("Attempting to reconnect...")
                
            except asyncio.CancelledError:
                # Clean shutdown
                logging.info("Socket manager shutting down")
                for task in self.processor_tasks:
                    task.cancel()
                await asyncio.gather(*self.processor_tasks, return_exceptions=True)
                raise

    async def process_socket_message(self, msg):
        """Process incoming ticker messages with optimized performance"""
        try:
            data = msg.get('data')
            if not data:
                return

            # Extract values from ticker data
            symbol = data.get('s')
            bid_price_str = data.get('b')  # Best bid price
            ask_price_str = data.get('a')  # Best ask price
            last_price_str = data.get('c')  # Last price
            timestamp = data.get('E', int(time.time() * 1000))
            
            if not (symbol and bid_price_str and ask_price_str):
                return

            # Convert prices using cached Decimal conversion
            bid_price = self.get_decimal_price(bid_price_str)
            ask_price = self.get_decimal_price(ask_price_str)
            last_price = self.get_decimal_price(last_price_str)
            
            # Update prices dictionary
            self.prices[symbol] = {
                'bid': bid_price,
                'ask': ask_price,
                'last_price': last_price,
                'update_time': timestamp
            }

            # Check for trading readiness
            if not self.socket_started and len(self.prices) >= 2:
                self.socket_started = True
                self.ready_to_trade.set()
            
            # Check arbitrage if socket is started and cooldown period has passed
            if self.socket_started and time.time() - self.last_trade_time >= self.trade_cooldown:
                await self.check_arbitrage()
                
        except Exception as e:
            logging.error(f"Error processing socket message: {e}")

    async def check_balances(self):
        """Verify sufficient balances for trading"""
        try:
            usdt_balance = Decimal((await self.client.get_asset_balance(asset='USDT'))['free'])
            usdc_balance = Decimal((await self.client.get_asset_balance(asset='USDC'))['free'])
            
            if usdt_balance < self.position_size:
                logging.warning(f"Insufficient USDT balance: {usdt_balance}")
                return False
                
            if usdc_balance < self.position_size:
                logging.warning(f"Insufficient USDC balance: {usdc_balance}")
                return False
                
            return True
            
        except Exception as e:
            logging.error(f"Error checking balances: {e}")
            return False

    async def calculate_trade_amount(self, price):
        """Calculate the amount of TRUMP to trade based on fixed position size"""
        try:
            trump_amount = self.position_size / price
                      
            # Normalize the amount according to step size
            decimal_places = abs(Decimal(self.step_size).as_tuple().exponent)
            trump_amount = (trump_amount // self.step_size) * self.step_size
            trump_amount = trump_amount.quantize(Decimal('0.' + '0' * decimal_places))
            
            return trump_amount
            
        except Exception as e:
            logging.error(f"Error calculating trade amount: {e}")
            return None

    async def check_arbitrage(self):
        """Check for arbitrage opportunities using socket data"""
        try:
            if time.time() - self.last_trade_time < self.trade_cooldown:
                return
                
            usdt_data = self.prices.get(self.pair1)
            usdc_data = self.prices.get(self.pair2)
            
            if not (usdt_data and usdc_data):
                return
                
            # Calculate potential profit percentages
            buy_usdt_sell_usdc = (
                usdc_data['bid'] - usdt_data['ask']
            ) / usdt_data['ask'] * self.DECIMAL_100
            
            buy_usdc_sell_usdt = (
                usdt_data['bid'] - usdc_data['ask']
            ) / usdc_data['ask'] * self.DECIMAL_100
            
            # Log opportunities
            logging.info(f"USDT-USDC spread: {buy_usdt_sell_usdc:.2f}%")
            logging.info(f"USDC-USDT spread: {buy_usdc_sell_usdt:.2f}%")
            
            if self.simulation:
                if buy_usdt_sell_usdc > self.min_profit_threshold_decimal:
                    logging.info(f"SIMULATION: Would execute USDT->USDC arbitrage with {buy_usdt_sell_usdc:.2f}% profit")
                elif buy_usdc_sell_usdt > self.min_profit_threshold_decimal:
                    logging.info(f"SIMULATION: Would execute USDC->USDT arbitrage with {buy_usdc_sell_usdt:.2f}% profit")
                return
                
            # Check balances before proceeding
            if not await self.check_balances():
                return
                
            # Execute trades if profit threshold met
            if buy_usdt_sell_usdc > self.min_profit_threshold_decimal:
                trump_amount = await self.calculate_trade_amount(usdt_data['ask'])
                await self.execute_arbitrage(
                    buy_pair=self.pair1,
                    sell_pair=self.pair2,
                    buy_price=usdt_data['ask'],
                    sell_price=usdc_data['bid'],
                    quantity=trump_amount
                )
                
            elif buy_usdc_sell_usdt > self.min_profit_threshold_decimal:
                trump_amount = await self.calculate_trade_amount(usdc_data['ask'])
                await self.execute_arbitrage(
                    buy_pair=self.pair2,
                    sell_pair=self.pair1,
                    buy_price=usdc_data['ask'],
                    sell_price=usdt_data['bid'],
                    quantity=trump_amount
                )
                
        except Exception as e:
            logging.error(f"Error in check_arbitrage: {e}")

    async def execute_arbitrage(self, buy_pair, sell_pair, buy_price, sell_price, quantity):
        """Execute arbitrage trades"""
        try:
            # Double-check current prices from socket
            current_prices = self.prices
            buy_pair_data = current_prices.get(buy_pair)
            
            if not buy_pair_data:
                return
                
            price_movement = abs(Decimal(buy_pair_data['ask']) - buy_price) / buy_price * self.DECIMAL_100
            if price_movement > self.max_slippage_decimal:
                logging.warning(f"Price moved too much: {price_movement}%")
                return
                
            # Execute buy order
            buy_order = await self.client.create_order(
                symbol=buy_pair,
                side='BUY',
                type='MARKET',
                quantity=quantity
            )
            
            if not buy_order:
                return
                
            # Small delay to ensure first order processes
            await asyncio.sleep(0.1)
            
            # Verify TRUMP balance
            trump_balance = Decimal((await self.client.get_asset_balance(asset='TRUMP'))['free'])
            if trump_balance < quantity:
                logging.error("Failed to receive TRUMP tokens")
                return
                
            # Execute sell order
            sell_order = await self.client.create_order(
                symbol=sell_pair,
                side='SELL',
                type='MARKET',
                quantity=quantity
            )
            
            if sell_order:
                profit = (sell_price - buy_price) * quantity
                logging.info(f"Arbitrage completed. Profit: {profit} USD")
                self.last_trade_time = time.time()
                
        except Exception as e:
            logging.error(f"Error executing arbitrage: {e}")

    async def start(self):
        """Start the arbitrage bot with enhanced error handling"""
        await self.initialize()
        logging.info(f"Starting {'simulation' if self.simulation else 'live'} arbitrage bot...")
        
        try:
            # Start socket and wait for initial data
            socket_task = asyncio.create_task(self.socket_manager())
            
            # Wait for socket and processors
            await asyncio.gather(
                socket_task,
                self.ready_to_trade.wait(),
                return_exceptions=True
            )
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
        finally:
            # Cleanup processor tasks
            for task in self.processor_tasks:
                task.cancel()
            await asyncio.gather(*self.processor_tasks, return_exceptions=True)

    async def close(self):
        """Enhanced cleanup with task cancellation"""
        # Cancel all processor tasks
        for task in self.processor_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self.processor_tasks:
            await asyncio.gather(*self.processor_tasks, return_exceptions=True)
        
        # Close client connection
        await self.client.close_connection()

async def main():
    # Create bot instance (True for testnet, False for mainnet)
    bot = HybridArbitrageBot(simulation=False, testnet=False)
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        logging.info("Shutting down bot...")
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())