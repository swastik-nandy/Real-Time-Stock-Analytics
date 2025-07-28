# app/models/StockPriceHistory.py

from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from app.db.database import Base

class StockPriceHistory(Base):
    __tablename__ = "stock_price_history"

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    price = Column(Float, nullable=False) 
    trade_time_stamp = Column(DateTime, nullable=False, index=True)
