from sqlalchemy import Column, Integer, String, DateTime, DECIMAL, Text
from sqlalchemy.sql import func
from .database import Base

class PacketData(Base):
    __tablename__ = "packet_data"
    
    id = Column(Integer, primary_key=True, index=True)
    packet_number = Column(Integer, index=True)
    timestamp = Column(String(50))
    source_ip = Column(String(20))
    destination_ip = Column(String(20))
    source_port = Column(Integer)
    destination_port = Column(Integer)
    packet_size = Column(Integer)
    protocol = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class ProtocolStats(Base):
    __tablename__ = "protocol_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    protocol_name = Column(String(20))
    packet_count = Column(Integer)
    total_size = Column(Integer)
    avg_size = Column(DECIMAL(10, 2))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class IPStats(Base):
    __tablename__ = "ip_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    ip_address = Column(String(20))
    role = Column(String(15))
    packet_count = Column(Integer)
    total_traffic = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())