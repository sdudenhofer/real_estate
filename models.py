from sqlmodel import SQLModel, Field

class redfinData(SQLModel, table=True):
    regionID: int | None = Field(primary_key=True)
    size_rank: int | None = Field(default=None)
    RegionName: str | None = Field(index=True)
    RegionType: str
    stateName: str
    state: str
    metro: str
    county: str

class sales(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    regionID: int | None = Field(foreign_key="redfinData.regionID")
    year: int | None = Field(default=None)
    january: float | None = Field(default=None)
    february: float | None = Field(default=None)
    march: float | None = Field(default=None)
    april: float | None = Field(default=None)
    may: float | None = Field(default=None)
    june: float | None = Field(default=None)
    july: float | None = Field(default=None)
    august: float | None = Field(default=None)
    september: float | None = Field(default=None)
    october: float | None = Field(default=None)
    november: float | None = Field(default=None)
    december: float | None = Field(default=None)

class latlong_data(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    regionID: int | None = Field(foreign_key="redfinData.regionID")
    latitude: float
    longitutde: float

