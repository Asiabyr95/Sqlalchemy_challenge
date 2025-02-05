from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import datetime as dt

# Database setup
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Station = Base.classes.station
Measurement = Base.classes.measurement

app = Flask(__name__)


@app.route("/")
def homepage():
    """
    List all the available routes.
    """
    return """
        <h1>Hawaii Climate Analysis</h1>
        <hr/>
        <h2> Available Routes </h2>
        <ul>
            <li><a href="/">Homepage</a></li>
            <li><a href="/api/v1.0/precipitation">Precipitation</a></li>
            <li><a href="/api/v1.0/stations">All Stations</a></li>
            <li><a href="/api/v1.0/tobs">Temperature observations for the most active station</a></li>
            <li><a href="/api/v1.0/start/2017-01-01">Calculate min, avg, and max temperatures for the dates from the start date to the end of the dataset</a></li>
            <li><a href="/api/v1.0/end/2016-01-01/2017-12-12">Calculate min, avg, and max temperatures for the dates from the start date to the end date, inclusive.</a></li>
        </ul>
    """


@app.route("/api/v1.0/precipitation")
def precipitation():
    """
    Convert the query results from your precipitation analysis to a
    dictionary using date as the key and prcp as the value.
    """
    session = Session(engine)

    # Query to retrieve the last 12 months of precipitation data,
    # starting from the most recent data point in the database.
    recent_date = session.query(func.max(Measurement.date)).scalar()
    recent_date = dt.datetime.strptime(recent_date, "%Y-%m-%d")

    # Calculate the date one year from the last date in data set.
    last_year_date = recent_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    data_last_year = (
        session.query(Measurement.date, Measurement.prcp)
        .filter(Measurement.date >= last_year_date)
        .all()
    )

    # close the session
    session.close()

    return jsonify({date: prcp for date, prcp in data_last_year})


@app.route("/api/v1.0/stations")
def all_stations():
    """
    Return a JSON list of stations from the dataset.
    """

    # Create a session to the database
    session = Session(engine)

    # Query to fetch all the stations in the dataset
    stations = session.query(Station.station).all()

    # Close the session
    session.close()

    # stations is a list of tuples, so convert to list of station names.
    stations = [station[0] for station in stations]

    return jsonify(stations)


@app.route("/api/v1.0/tobs")
def tobs():
    """
    Return a JSON list of temperature observations for the previous year.
    """

    # Create a session
    session = Session(engine)

    # Find the most active station ID

    # Query to fetch the all the station ids and their counts
    station_count_query = session.query(
        Measurement.station, func.count(Measurement.station))

    # groups the results by the station columns
    grouped_by_station = station_count_query.group_by(Measurement.station)

    # sort the results in descending order
    ordered_stations = grouped_by_station.order_by(
        func.count(Measurement.station).desc())

    # Fetch all the results
    most_active_stations = ordered_stations.all()

    # First element is the most active station
    most_active_station_id = most_active_stations[0][0]

    # Using the most active station id
    # Query the last 12 months of temperature observation data for this station and plot the results as a histogram

    # Query the Measurement table for the maximum date value.
    # Only include the station  which matches the most active station
    most_recent_date = session.query(func.max(Measurement.date)).filter(
        Measurement.station == most_active_station_id).scalar()

    # Convert the most recent date to python DateTime Object
    most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')

    # Get date 1 year ago from the most recent date
    last_year_date = most_recent_date - dt.timedelta(days=365)

    # Query the Measurement table to fetch two columns, date and tobs.
    # Only include station which matches the most active station and include only the rows
    # which where measured in the last year.
    temperature_data = session.query(Measurement.date, Measurement.tobs).filter(
        Measurement.station == most_active_station_id,
        Measurement.date >= last_year_date
    ).all()

    # Close the session
    session.close()

    data = [{"date": date, "temperature": tobs}
            for date, tobs in temperature_data]
    return jsonify(data)


@app.route("/api/v1.0/start/<start>")
def start_date(start):
    """
    Returns the min, max, and average temperatures calculated from the given start date to the end of the dataset
    """
    # Create a session
    session = Session(engine)

    # Query min, avg, max temperatures for dates greater than or equal to the start date
    results = (
        session.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs)
        )
        .filter(Measurement.date >= start)
        .all()
    )

    # Close the session
    session.close()

    # Format the results into a dictionary
    data = {
        "min": results[0][0],
        "avg": round(results[0][1], 2),
        "max": results[0][2],
        "start": start
    }

    # Return the JSON representation
    return jsonify(data)


@app.route("/api/v1.0/end/<start>/<end>")
def start_end_date(start, end):
    """
    Returns the min, max, and average temperatures calculated from the given start date to the given end date
    """

    # Create a session
    session = Session(engine)

    # Query min, avg, max for dates between start and end (inclusive)
    results = (
        session.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs)
        )
        .filter(Measurement.date >= start)
        .filter(Measurement.date <= end)
        .all()
    )

    # Close the session
    session.close()

    # Format the results into a dictionary
    data = {
        "min": results[0][0],
        "avg": round(results[0][1], 2),
        "max": results[0][2],
        "start": start,
        "end": end
    }

    # Return the JSON representation
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
