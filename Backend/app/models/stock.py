# app/models/stock.py

from sqlalchemy import Column, Integer, String
from app.db.database import Base

class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, index=True, nullable=False)
    name = Column(String)
    type = Column(String, nullable=False)  # e.g., 'crypto', 'forex', 'stock'
