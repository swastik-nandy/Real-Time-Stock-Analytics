# app/models/PredictedPrices.py

from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from app.db.database import Base

class PredictedPrices(Base):
    __tablename__ = "predicted_prices"

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))
    predicted_price = Column(Float)
    prediction_time = Column(DateTime)   # The time this prediction is for
    generated_at = Column(DateTime)      # When this prediction was made
