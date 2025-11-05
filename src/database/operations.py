from sqlalchemy import desc, func, and_
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
from .models import DatabaseManager, Trip, CanceledTrip, ScrapingSession, DatabaseMetrics
from ..utils.logger import logger
from ..utils.monitoring import track_execution_time

class DataOperations:
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(SQLAlchemyError)
    )
    @track_execution_time
    def trip_exists(self, trip_id: str) -> bool:
        """Check if a trip already exists in database"""
        with self.db_manager.get_session() as session:
            return session.query(Trip).filter(Trip.trip_id == trip_id).first() is not None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(SQLAlchemyError)
    )
    @track_execution_time
    def save_trip(self, trip_data: dict) -> bool:
        """Save trip data to database"""
        try:
            with self.db_manager.get_session() as session:
                # Check if trip already exists
                existing = session.query(Trip).filter(Trip.trip_id == trip_data['trip_id']).first()
                
                if existing:
                    # Update existing record
                    for key, value in trip_data.items():
                        setattr(existing, key, value)
                    existing.updated_at = datetime.utcnow()
                else:
                    # Create new record
                    trip = Trip(**trip_data)
                    session.add(trip)
                
                session.commit()
                logger.info(f"Saved trip {trip_data['trip_id']}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save trip {trip_data.get('trip_id')}: {e}")
            return False
    
    @track_execution_time
    def save_canceled_trip(self, canceled_data: dict) -> bool:
        """Save canceled trip data"""
        try:
            with self.db_manager.get_session() as session:
                canceled_trip = CanceledTrip(**canceled_data)
                session.add(canceled_trip)
                session.commit()
                logger.info(f"Saved canceled trip {canceled_data['trip_id']}")
                return True
        except Exception as e:
            logger.error(f"Failed to save canceled trip {canceled_data.get('trip_id')}: {e}")
            return False
    
    @track_execution_time
    def get_last_scraped_date(self) -> Optional[datetime]:
        """Get the date of the last scraped trip"""
        with self.db_manager.get_session() as session:
            last_trip = session.query(Trip).order_by(desc(Trip.date)).first()
            return last_trip.date if last_trip else None
    
    @track_execution_time
    def start_scraping_session(self) -> int:
        """Start a new scraping session and return session ID"""
        with self.db_manager.get_session() as session:
            scraping_session = ScrapingSession(status='started')
            session.add(scraping_session)
            session.commit()
            logger.info(f"Started scraping session {scraping_session.id}")
            return scraping_session.id
    
    @track_execution_time
    def complete_scraping_session(self, session_id: int, trips_count: int, 
                                canceled_count: int, last_date: datetime,
                                duration: float, error_message: str = None):
        """Mark scraping session as completed"""
        with self.db_manager.get_session() as session:
            scraping_session = session.query(ScrapingSession).get(session_id)
            if scraping_session:
                scraping_session.status = 'completed' if not error_message else 'failed'
                scraping_session.trips_scraped = trips_count
                scraping_session.canceled_trips_scraped = canceled_count
                scraping_session.last_trip_date = last_date
                scraping_session.duration_seconds = duration
                scraping_session.error_message = error_message
                session.commit()
                logger.info(f"Completed scraping session {session_id}")
    
    @track_execution_time
    def get_earnings_summary(self, start_date: datetime, end_date: datetime) -> dict:
        """Get earnings summary for date range"""
        with self.db_manager.get_session() as session:
            result = session.query(
                func.count(Trip.trip_id),
                func.sum(Trip.earnings),
                func.avg(Trip.earnings)
            ).filter(
                and_(
                    Trip.date >= start_date,
                    Trip.date <= end_date,
                    Trip.is_canceled == False
                )
            ).first()
            
            return {
                'total_trips': result[0] or 0,
                'total_earnings': result[1] or 0.0,
                'average_earnings': result[2] or 0.0
            }
    
    @track_execution_time
    def update_database_metrics(self):
        """Update database metrics table"""
        with self.db_manager.get_session() as session:
            # Calculate metrics
            total_trips = session.query(func.count(Trip.trip_id)).scalar()
            earnings_data = session.query(
                func.sum(Trip.earnings),
                func.avg(Trip.earnings)
            ).filter(Trip.is_canceled == False).first()
            
            metrics = DatabaseMetrics(
                metric_date=datetime.utcnow(),
                total_trips=total_trips,
                total_earnings=earnings_data[0] or 0.0,
                avg_earnings_per_trip=earnings_data[1] or 0.0,
                success_rate=1.0  # Placeholder for actual success rate calculation
            )
            
            session.add(metrics)
            session.commit()