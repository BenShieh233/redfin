from sqlalchemy import create_engine, Column, String, Text, Date, Float, Integer, Boolean, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Index

# 创建基础类
Base = declarative_base()

# 数据库表模型
class Review(Base):
    __tablename__ = 'reviews'

    itemId = Column(String(50), primary_key=True)
    current_url = Column(Text)
    ReviewText = Column(Text)
    SubmissionTime = Column(Date)

    __table_args__ = (UniqueConstraint('itemId', 'current_url', name='_item_current_uc'),)

# 数据库表模型
class Review_LCF(Base):
    __tablename__ = 'reviews_LCF'

    itemId = Column(String(50), primary_key=True)
    current_url = Column(Text)
    ReviewText = Column(Text)
    SubmissionTime = Column(Date)

    # 创建索引，指定current_url前255个字符
    __table_args__ = (
        Index('idx_item_current_url', 'itemId', 'current_url', mysql_length={'current_url': 255}, unique=True),
    )


# 数据库表模型 - CeilingFanWithLight
class CeilingFanWithLight(Base):
    __tablename__ = 'ceiling_fans_with_lights'

    id = Column(Integer, primary_key=True, autoincrement=True)
    Order = Column(Integer)
    label = Column(String(255))
    isSponsored = Column(Boolean, nullable=True)
    itemId = Column(String(50))
    canonicalUrl = Column(String(255))
    brandName = Column(String(100))
    productLabel = Column(String(255))
    storeSkuNumber = Column(String(50))
    parentId = Column(String(50))
    modelNumber = Column(String(50))
    price = Column(Float, nullable=True)
    original_price = Column(Float, nullable=True)
    averageRating = Column(Float, nullable=True)
    totalReviews = Column(Integer, nullable=True)
    inventory = Column(Integer, nullable=True)
    scraped_time = Column(Date)
    
# 数据库表模型 - CeilingFanWithLight
class CeilingFan(Base):
    __tablename__ = 'ceiling_fans'

    id = Column(Integer, primary_key=True, autoincrement=True)
    Order = Column(Integer)
    label = Column(String(255))
    isSponsored = Column(Boolean, nullable=True)
    itemId = Column(String(50))
    canonicalUrl = Column(String(255))
    brandName = Column(String(100))
    productLabel = Column(String(255))
    storeSkuNumber = Column(String(50))
    parentId = Column(String(50))
    modelNumber = Column(String(50))
    price = Column(Float, nullable=True)
    original_price = Column(Float, nullable=True)
    averageRating = Column(Float, nullable=True)
    totalReviews = Column(Integer, nullable=True)
    inventory = Column(Integer, nullable=True)
    scraped_time = Column(Date)
    
# 数据库连接设置
DATABASE_URL = "mysql+mysqlconnector://root:0803@localhost:3306/homedepot"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
# 创建表（如果表不存在）
Base.metadata.create_all(engine)

