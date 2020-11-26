CREATE TABLE IF NOT EXISTS staging_immigrations ( 
    id           INT NOT NULL,
    year         INT NOT NULL,
    month        INT NOT NULL,
    fromCountry  INT NOT NULL, 
    destCountry  INT NOT NULL, 
    gender       CHAR(1), 
    age          INT NOT NULL,
    birth_year       INT NOT NULL,
    admission_number BIGINT NOT NULL,
    arrive_date      DATE NOT NULL,
    departure_date   DATE NOT NULL,
    flight_number    NVARCHAR(10) NOT NULL,
    airline          NVARCHAR(5) NOT NULL,
    reside_in        NVARCHAR(2) NOT NULL,
    transportation_mode   INT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_temperatures ( 
    Id                             INT IDENTITY (0, 1) NOT NULL,
    Date                           DATE NOT NULL,
    AverageTemperature             DECIMAL(6, 3) NOT NULL,
    AverageTemperatureUncertainty  DECIMAL(6, 3) NOT NULL, 
    City                           NVARCHAR(50) NOT NULL, 
    Country                        NVARCHAR(50) NOT NULL, 
    Latitude                       NVARCHAR(7) NOT NULL,
    Longitude                      NVARCHAR(7) NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_demographics ( 
    Id                      INT IDENTITY (0, 1) NOT NULL,
    City                    NVARCHAR(50) NOT NULL,
    State                   NVARCHAR(50) NOT NULL,
    MedianAge               DECIMAL(6, 3) NOT NULL, 
    MalePopulation          INT NOT NULL, 
    FemalePopulation        INT NOT NULL, 
    TotalPopulation         INT NOT NULL,
    NumberVeterans          INT NOT NULL,
    ForeignBorn             INT NOT NULL,
    AverageHouseholdSize    DECIMAL(6, 3) NOT NULL,
    Race                    NVARCHAR(256) NOT NULL,
    Count                   INT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_airports ( 
    id              NVARCHAR(10) NOT NULL,
    type            NVARCHAR(50) NOT NULL,
    name            NVARCHAR(100) NOT NULL,
    elevation_ft    INT NOT NULL, 
    country         NVARCHAR(5) NOT NULL, 
    region          NVARCHAR(10) NOT NULL, 
    latitudes       NVARCHAR(50) NOT NULL,
    longitudes      NVARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Flights ( 
    FlightNumber NVARCHAR(10) NOT NULL PRIMARY KEY,
    AirlineName  NVARCHAR(5)  NOT NULL,
    UNIQUE(FlightNumber)
);

CREATE TABLE IF NOT EXISTS TransportationModes ( 
    TransportationCode INT          NOT NULL PRIMARY KEY,
    Description        NVARCHAR(20) NOT NULL,
    UNIQUE(TransportationCode)
);

CREATE TABLE IF NOT EXISTS Countries ( 
    Id               INT IDENTITY (0, 1) NOT NULL PRIMARY KEY,
    CountryName      NVARCHAR(50) NOT NULL,
    CountryShortName NVARCHAR(2) NULL,
    CountryCode      NVARCHAR(3) NULL,    
    UNIQUE(Id)
);

CREATE TABLE IF NOT EXISTS Dates ( 
    Id    INT IDENTITY (0, 1) NOT NULL PRIMARY KEY,
    Date  DATE NOT NULL,
    UNIQUE(Id)
);

CREATE TABLE IF NOT EXISTS USAStates ( 
    StateCode   NVARCHAR(2) NOT NULL PRIMARY KEY,
    StateName   NVARCHAR(50) NOT NULL,
    UNIQUE(StateCode)
);

CREATE TABLE IF NOT EXISTS Cities ( 
    Id INT IDENTITY (0, 1) NOT NULL PRIMARY KEY,
    CityName NVARCHAR(50) NOT NULL,
    UNIQUE(Id)
);

CREATE TABLE IF NOT EXISTS Coordinates ( 
    Id        INT IDENTITY (0, 1) NOT NULL PRIMARY KEY,
    Latitude  NVARCHAR(50) NOT NULL,
    Longitude NVARCHAR(50) NOT NULL,
    UNIQUE(Id)
);

CREATE TABLE IF NOT EXISTS Regions ( 
    Id         INT IDENTITY (0, 1) NOT NULL PRIMARY KEY,
    RegionCode NVARCHAR(10) NOT NULL,
    UNIQUE(Id)
);

CREATE TABLE IF NOT EXISTS Immigrations(
    Id                 BIGINT NOT NULL PRIMARY KEY,
    Year               INT NOT NULL,
    Month              INT NOT NULL,
    FromCountryId      INT,
    DestCountryId      INT,
    Gender             CHAR(1) NOT NULL,
    Age                INT NOT NULL,
    BirthYear          INT NOT NULL,
    AdmissionNumber    BIGINT NOT NULL,
    ArriveDateId       INT NOT NULL,
    DepartureDateId    INT NOT NULL,
    FlightNumber       NVARCHAR(10) NOT NULL,
    ResideInState      NVARCHAR(2) NOT NULL,
    TransportationMode INT NOT NULL,
    UNIQUE(Id),
    CONSTRAINT fk_fromcountry         FOREIGN KEY(FromCountryId)      REFERENCES Countries(Id),
    CONSTRAINT fk_destcountry         FOREIGN KEY(DestCountryId)      REFERENCES Countries(Id),
    CONSTRAINT fk_arrivedate          FOREIGN KEY(ArriveDateId)       REFERENCES Dates(Id),
    CONSTRAINT fk_departuredate       FOREIGN KEY(DepartureDateId)    REFERENCES Dates(Id),
    CONSTRAINT fk_flight              FOREIGN KEY(FlightNumber)       REFERENCES Flights(FlightNumber),
    CONSTRAINT fk_resideinstate       FOREIGN KEY(ResideInState)      REFERENCES USAStates(StateCode),
    CONSTRAINT fk_transportationmode  FOREIGN KEY(TransportationMode) REFERENCES TransportationModes(TransportationCode)
);

CREATE TABLE IF NOT EXISTS Temperatures(
    Id                            BIGINT IDENTITY (0, 1) NOT NULL PRIMARY KEY, 
    DateId                        BIGINT NOT NULL, 
    AverageTemperature            DECIMAL(6, 3) NOT NULL,
    AverageTemperatureUncertainty DECIMAL(6, 3) NOT NULL,
    CityId                        INT NOT NULL,
    CountryId                     INT NOT NULL,
    CoordinateId                  INT NOT NULL,
    UNIQUE(Id),
    CONSTRAINT fk_date         FOREIGN KEY(DateId)         REFERENCES Dates(Id),
    CONSTRAINT fk_city         FOREIGN KEY(CityId)         REFERENCES Cities(Id),
    CONSTRAINT fk_country      FOREIGN KEY(CountryId)      REFERENCES Countries(Id),
    CONSTRAINT fk_coordinate   FOREIGN KEY(CoordinateId)   REFERENCES Coordinates(Id)
);


CREATE TABLE IF NOT EXISTS Demographics(
    Id                      BIGINT IDENTITY (0, 1) NOT NULL PRIMARY KEY, 
    CityId                  INT NOT NULL, 
    StateId                 NVARCHAR(2) NOT NULL, 
    MedianAge               DECIMAL(6, 3) NOT NULL,
    MalePopulation          INT NOT NULL,
    FemalePopulation        INT NOT NULL,
    TotalPopulation         INT NOT NULL,
    NumberVeterans          INT NOT NULL,
    ForeignBorn             INT NOT NULL,
    AverageHouseHoldSize    DECIMAL(6, 3) NOT NULL,
    Race                    NVARCHAR(256) NOT NULL,
    Count                   INT NOT NULL,
    UNIQUE(Id),
    CONSTRAINT fk_city      FOREIGN KEY(CityId)    REFERENCES Cities(Id),
    CONSTRAINT fk_state     FOREIGN KEY(StateId)   REFERENCES USAStates(StateCode)
);

CREATE TABLE IF NOT EXISTS Airports(
    Id   NVARCHAR(10) NOT NULL PRIMARY KEY, 
    Type NVARCHAR(20) NOT NULL,
    Name NVARCHAR(100) NOT NULL,
    Elevation_ft INT NOT NULL,
    RegionId INT NOT NULL,
    CountryId INT NOT NULL,
    CoordinateId INT NOT NULL,
    UNIQUE(Id),
    CONSTRAINT fk_region      FOREIGN KEY(RegionId)     REFERENCES Regions(Id),
    CONSTRAINT fk_country     FOREIGN KEY(CountryId)    REFERENCES Countries(Id),
    CONSTRAINT fk_coordinate  FOREIGN KEY(CoordinateId) REFERENCES Coordinates(Id)
);





