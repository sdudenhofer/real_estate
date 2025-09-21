from sqlmodel import SQLModel, Field, Relationship
from typing import Optional

class geoData(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    city: str = Field(max_length=100)
    state: str = Field(max_length=2)
    latitude: str = Field(max_length=10)
    longitude: str = Field(max_length=10)


class RealEstateData(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    region_name: str = Field(max_length=100)
    state: str = Field(max_length=2)
    date: str = Field(max_length=10)
    zhvi: Optional[float] = None
    geo_data_id: Optional[int] = Field(default=None, foreign_key="geodata.id")
    geo_data: Optional[geoData] = Relationship(back_populates="real_estate_data")

geoData.real_estate_data = Relationship(back_populates="geo_data", sa_relationship_kwargs={"cascade": "all, delete-orphan"})


class distanceData(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    origin_id: int = Field(foreign_key="geodata.id")
    destination_id: int = Field(foreign_key="geodata.id")
    distance_miles: Optional[float] = None
    origin: Optional[geoData] = Relationship(sa_relationship_kwargs={"foreign_keys": "[distanceData.origin_id]"})
    destination: Optional[geoData] = Relationship(sa_relationship_kwargs={"foreign_keys": "[distanceData.destination_id]"})