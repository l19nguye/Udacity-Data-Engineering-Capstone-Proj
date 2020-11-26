class SqlQueries:
    insert_dimension_coordinates = ("""
        insert into Coordinates(latitude, longitude)
        select distinct latitudes, longitudes from staging_airports
        union
        select distinct latitude, longitude from staging_temperatures
    """)

    insert_dimension_regions = ("""
        insert into Regions(RegionCode)
        select distinct region from staging_airports
    """)

    insert_dimension_cities = ("""
        insert into Cities(CityName) 
        ( select distinct city from staging_demographics
          union
          select distinct city from staging_temperatures )
    """)

    insert_dimension_dates= ("""
        insert into Dates(Date) 
        select distinct date from staging_temperatures
        union
        select distinct arrive_date from staging_immigrations
        union
        select distinct departure_date from staging_immigrations
    """)

    insert_dimension_flights = ("""
        insert into Flights(FlightNumber, AirlineName) 
        select distinct flight_number, airline from staging_immigrations
    """)
    
    insert_dimension_transportations = ("""
        insert into TransportationModes(TransportationCode, Description) 
        select 1, 'Air'
        union
        select 2, 'Sea'
        union
        select 3, 'Land'
        union
        select 9, 'Not reported'
    """)
    
    insert_fact_temperatures = ("""
        insert into Temperatures(DateId, AverageTemperature, AverageTemperatureUncertainty, CityId, CountryId, CoordinateId)
        select d.Id as DateId, s.AverageTemperature, s.AverageTemperatureUncertainty, c.Id as CityId,
            o.Id as CountryId, r.Id as CoordinateId
        from staging_temperatures s
        join Dates d on s.Date = d.Date
        join Cities c on s.City = c.CityName
        join Countries o on s.Country = o.CountryName
        join Coordinates r on s.Latitude = r.Latitude
        and s.Longitude = r.Longitude
    """)
    
    insert_fact_immigrations = ("""
        insert into Immigrations
        select i.Id, i.Year, i.Month, c1.Id as FromCountryCode, c2.Id as DestCountryCode,
                    i.gender, i.age, i.birth_year as BirthYear, i.admission_number as AdmissionNumber, 
                    d1.Id as ArriveDateId, d2.Id as DepartureDateId,
                    f.FlightNumber, u.StateCode as ResideInState, t.TransportationCode as TransportationMode
            from staging_immigrations i
            join Countries c1 on i.fromCountry = c1.CountryCode
            join Countries c2 on i.destCountry = c2.CountryCode
            join Dates d1 on i.arrive_date = d1.Date
            join Dates d2 on i.departure_date = d2.Date
            join Flights f on i.flight_number = f.FlightNumber
            join USAStates u on i.reside_in = u.StateCode
            join TransportationModes t on i.transportation_mode = t.TransportationCode
    """)
    
    insert_fact_airports= ("""
        insert into Airports(Id, Type, Name, Elevation_ft, RegionId, CountryId, CoordinateId)
        select a.Id, a.Type, a.Name, a.Elevation_ft, r.Id as RegionId,
                c.Id as CountryId, d.Id as CoordinateId
        from staging_airports a 
        join Regions r on a.Region = r.RegionCode
        join Countries c on a.country = c.CountryShortName
        join Coordinates d on a.latitudes = d.Latitude
        and a.longitudes = d.Longitude
    """)
    
    insert_fact_demographics= ("""
        insert into Demographics(CityId, StateId, MedianAge, MalePopulation, FemalePopulation,
                        TotalPopulation, NumberVeterans, ForeignBorn, AverageHouseholdSize, Race, Count)
        select c.Id as CityId, u.StateCode as StateId, d.MedianAge, d.MalePopulation, d.FemalePopulation,
            d.TotalPopulation, d.NumberVeterans, d.ForeignBorn, d.AverageHouseholdSize, d.Race, d.Count
        from staging_demographics d
        join Cities c on d.City = c.CityName
        join USAStates u on u.StateName = d.State
    """)