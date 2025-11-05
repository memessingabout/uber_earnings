from playwright.sync_api import Page
from datetime import datetime
import time
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ..utils.logger import logger
from ..utils.date_utils import get_uber_week_range, is_current_week
from ..utils.monitoring import track_execution_time
from ..database.operations import DataOperations
from .exceptions import UberScrapingError, UberLoginRequired, UberRateLimit

class ActivitiesScraper:
    def __init__(self, page: Page):
        self.page = page
        self.data_ops = DataOperations()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((UberScrapingError, TimeoutError))
    )
    @track_execution_time
    def select_week(self, target_date: datetime) -> bool:
        """Select specific week in date picker"""
        try:
            # Open date picker
            date_selector = '[data-testid="date-picker"]'
            if not self.page.wait_for_selector(date_selector, timeout=10000):
                raise UberScrapingError("Date picker not found")
            
            self.page.click(date_selector)
            
            # Wait for calendar to open
            self.page.wait_for_selector('[data-baseweb="calendar"]', timeout=10000)
            
            # Navigate to target month/year
            if not self._navigate_to_month(target_date):
                return False
            
            # Select date
            date_cell = self.page.query_selector(f'[aria-label*="{target_date.strftime("%B %d, %Y")}"]')
            if date_cell:
                date_cell.click()
                logger.info(f"Selected week containing {target_date.strftime('%Y-%m-%d')}")
                time.sleep(2)  # Wait for page to load
                return True
            
            logger.error(f"Could not find date {target_date.strftime('%Y-%m-%d')} in picker")
            return False
            
        except Exception as e:
            logger.error(f"Error selecting week: {e}")
            raise UberScrapingError(f"Failed to select week: {e}")
    
    @track_execution_time
    def _navigate_to_month(self, target_date: datetime) -> bool:
        """Navigate calendar to target month"""
        try:
            current_month = self.page.query_selector('[data-baseweb="calendar"] [aria-live="polite"]')
            if not current_month:
                return False
            
            current_text = current_month.inner_text()
            target_text = target_date.strftime("%B %Y")
            
            max_navigation = 12  # Maximum months to navigate
            
            for _ in range(max_navigation):
                if target_text in current_text:
                    return True
                
                # Determine navigation direction
                current_date = datetime.strptime(current_text, "%B %Y")
                if target_date < current_date:
                    nav_btn = self.page.query_selector('[aria-label="Previous month"]')
                else:
                    nav_btn = self.page.query_selector('[aria-label="Next month"]')
                
                if nav_btn:
                    nav_btn.click()
                    time.sleep(1)
                    current_text = current_month.inner_text()
                else:
                    break
            
            return target_text in current_text
            
        except Exception as e:
            logger.error(f"Error navigating to month: {e}")
            return False
    
    @track_execution_time
    def load_all_trips(self) -> List[Dict]:
        """Load all trips by clicking 'Load More' until it disappears"""
        trips = []
        max_attempts = 50  # Safety limit
        consecutive_failures = 0
        
        for attempt in range(max_attempts):
            try:
                # Extract current trips before loading more
                current_trips = self._extract_trips_from_page()
                if len(current_trips) == len(trips) and attempt > 0:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0
                    trips = current_trips
                
                # Check for load more button
                load_more_btn = self.page.query_selector('button:has-text("Load More")')
                
                if not load_more_btn or consecutive_failures >= 3:
                    logger.info("All trips loaded")
                    break
                
                # Click load more with retry
                if self._safe_click_load_more(load_more_btn):
                    logger.info(f"Loaded more trips, total: {len(trips)}")
                else:
                    consecutive_failures += 1
                
                # Add delay to be gentle
                time.sleep(config.scraping.request_delay)
                
            except Exception as e:
                logger.error(f"Error loading more trips (attempt {attempt + 1}): {e}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    break
        
        logger.info(f"Final trip count: {len(trips)}")
        return trips
    
    @track_execution_time
    def _safe_click_load_more(self, button) -> bool:
        """Safely click load more button with error handling"""
        try:
            # Scroll to button
            self.page.evaluate("(element) => element.scrollIntoView({behavior: 'smooth', block: 'center'})", button)
            time.sleep(0.5)
            
            # Click button
            button.click()
            
            # Wait for loading to complete
            self.page.wait_for_timeout(3000)
            
            return True
        except Exception as e:
            logger.warning(f"Failed to click load more: {e}")
            return False
    
    @track_execution_time
    def _extract_trips_from_page(self) -> List[Dict]:
        """Extract trip summaries from activities page"""
        trips = []
        
        try:
            # More specific selector for trip cards
            trip_cards = self.page.query_selector_all('[data-testid="trip-card"], [class*="trip-card"]')
            
            for card in trip_cards:
                try:
                    trip_data = self._parse_trip_card(card)
                    if trip_data and trip_data.get('trip_id'):
                        trips.append(trip_data)
                except Exception as e:
                    logger.warning(f"Failed to parse trip card: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting trips: {e}")
        
        return trips
    
    @track_execution_time
    def _parse_trip_card(self, card) -> Optional[Dict]:
        """Parse individual trip card data"""
        try:
            # Try multiple possible selectors for trip ID
            trip_id = (
                card.get_attribute('data-trip-id') or
                card.get_attribute('data-id') or
                card.get_attribute('id')
            )
            
            if not trip_id:
                return None
            
            # Extract other details
            date_element = card.query_selector('[data-testid="trip-date"], [class*="trip-date"]')
            earnings_element = card.query_selector('[data-testid="trip-earnings"], [class*="trip-earnings"]')
            details_button = card.query_selector('button:has-text("View Details"), [data-testid="view-details"]')
            
            return {
                'trip_id': trip_id,
                'date': self._parse_date(date_element.inner_text() if date_element else ''),
                'earnings': self._parse_currency(earnings_element.inner_text() if earnings_element else ''),
                'view_details_selector': self._get_view_details_selector(details_button, trip_id),
                'raw_card_data': card.inner_text()[:200]  # Store partial raw data for debugging
            }
            
        except Exception as e:
            logger.warning(f"Failed to parse trip card: {e}")
            return None
    
    @track_execution_time
    def _get_view_details_selector(self, button, trip_id: str) -> str:
        """Generate selector for view details button"""
        if button:
            # Try to get a reliable selector
            button_id = button.get_attribute('id')
            if button_id:
                return f'#{button_id}'
            
            button_class = button.get_attribute('class')
            if button_class:
                return f'[class="{button_class}"]'
        
        # Fallback selector
        return f'[data-trip-id="{trip_id}"] button'