from datetime import datetime
import pandas as pd
from coinmetrics.api_client import CoinMetricsClient
import json
from flask import Flask, jsonify
from flask_swagger import swagger
from compound import Compound
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
client = CoinMetricsClient()

@app.route("/")
def spec():
    swag = swagger(app)
    swag['info'] = []
    swag['definitions'] = []
    return jsonify(swag)

@app.route('/hello_world')
def hello_world():  # put application's code here
    """
            Hello, World!
            ---
            responses:
              200:
                description: Hello, World!
            """
    return 'Hello World!'


# create a route to get the list of assets
@app.route('/assets')
def get_assets():
    """
            Return s a list of all the assets you can get metrics for
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics("PriceUSD")[0]["frequencies"][0]["assets"]
    return json.dumps(assets)


# create a route to get the metrics for a specific asset
@app.route('/metrics/<asset>')
def get_metrics(asset):
    """
            Returns a list of all the prices for <asset>
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics()[0]["frequencies"][0]["assets"]
    if asset in assets:
        df_prices = client.get_asset_metrics(
            assets=[asset],
            metrics="PriceUSD",
            frequency="1d",
        ).to_dataframe()
        # Assign datatypes
        df_prices["time"] = pd.to_datetime(df_prices.time)
        # Convert time to string
        df_prices["time"] = df_prices["time"].dt.strftime("%Y-%m-%d")
        df_prices["Price"] = df_prices.PriceUSD.astype(float)
        # Reshape dataset so assets are in columns, dates are the rows, and the values are prices
        df_prices_pivot = df_prices.pivot(
            index="time",
            columns="asset",
            values="PriceUSD"
        )
        return json.dumps(df_prices_pivot[asset].to_dict())
    else:
        return "Asset not found"

# create a route to get the metrics for a specific asset and from a specific date
@app.route('/metrics/<asset>/<start_date>')
def get_metrics_from_date(asset, start_date):
    """
            Returns a list of the prices of <asset> from <start_date>
            asset: the asset you want to get metrics for
            start_date: the start date of the range you want to get metrics for
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics()[0]["frequencies"][0]["assets"]
    if asset in assets:
        df_prices = client.get_asset_metrics(
            assets=[asset],
            metrics="PriceUSD",
            frequency="1d",
            start_time=start_date
        ).to_dataframe()
        # Assign datatypes
        df_prices["time"] = pd.to_datetime(df_prices.time)
        # Convert time to string
        df_prices["time"] = df_prices["time"].dt.strftime("%Y-%m-%d")
        df_prices["Price"] = df_prices.PriceUSD.astype(float)
        # Reshape dataset so assets are in columns, dates are the rows, and the values are prices
        df_prices_pivot = df_prices.pivot(
            index="time",
            columns="asset",
            values="PriceUSD"
        )
        return json.dumps(df_prices_pivot[asset].to_dict())
    else:
        return "Asset not found"

# create a route to get the metrics for a specific asset and for a specific range of date
@app.route('/metrics/<asset>/<start_date>/<end_date>')
def get_metrics_date(asset, start_date, end_date):
    """
            Returns a list of the prices of <asset> from <start_date> to <end_date>
            asset: the asset you want to get metrics for
            start_date: the start date of the range you want to get metrics for
            end_date: the end date of the range you want to get metrics for
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics()[0]["frequencies"][0]["assets"]
    if asset in assets:
        df_prices = client.get_asset_metrics(
            assets=[asset],
            metrics="PriceUSD",
            frequency="1d",
            start_time=start_date,
            end_time=end_date
        ).to_dataframe()
        # Assign datatypes
        df_prices["time"] = pd.to_datetime(df_prices.time)
        # Convert time to string
        df_prices["time"] = df_prices["time"].dt.strftime("%Y-%m-%d")
        df_prices["Price"] = df_prices.PriceUSD.astype(float)
        # Reshape dataset so assets are in columns, dates are the rows, and the values are prices
        df_prices_pivot = df_prices.pivot(
            index="time",
            columns="asset",
            values="PriceUSD"
        )
        return json.dumps(df_prices_pivot[asset].to_dict())
    else:
        return "Asset not found"


# create a route to get the compound interest for a specific asset
@app.route('/compound/<asset>/<amount>/<period>')
def get_compound(asset, amount, period):
    """
            Returns a list of the compound interests you could have earned with <amount> $ spent each <period> in <asset> from its creation
            asset: the asset you want to invest in ( go to /assets to look at the list of assets )
            amount: the amount you want to invest each period ( int )
            period: the period you want to invest for ( "d", "w", "y" )
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics()[0]["frequencies"][0]["assets"]
    if asset in assets and period in ["d", "m", "y"]:
        df_prices = client.get_asset_metrics(
            assets=[asset],
            metrics="PriceUSD",
            frequency="1d",
        ).to_dataframe()
        # Assign datatypes
        df_prices["time"] = pd.to_datetime(df_prices.time)
        # Convert time to string
        df_prices["time"] = df_prices["time"].dt.strftime("%Y-%m-%d")
        df_prices["Price"] = df_prices.PriceUSD.astype(float)
        # Reshape dataset so assets are in columns, dates are the rows, and the values are prices
        df_prices_pivot = df_prices.pivot(
            index="time",
            columns="asset",
            values="PriceUSD"
        )
        # Calculate compound interest
        df = Compound(df_prices_pivot, int(amount), period, asset)

        return json.dumps(df["Compound"].to_dict())
    else:
        return "Asset not found"

# create a route to get the compound interest for a specific asset and from a specific date
@app.route('/compound/<asset>/<amount>/<period>/<start_date>')
def get_compound_date_from(asset, amount, period, start_date):
    """
            Returns a list of the compound interests you could have earned with <amount> $ spent each <period> in <asset> from <start_date>
            asset: the asset you want to invest in ( go to /assets to look at the list of assets )
            amount: the amount you want to invest each period ( int )
            period: the period you want to invest for ( "d", "w", "y" )
            start_date: the start date of the range you want to get compound interest for
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics()[0]["frequencies"][0]["assets"]
    if asset in assets and period in ["d", "m", "y"]:
        df_prices = client.get_asset_metrics(
            assets=[asset],
            metrics="PriceUSD",
            frequency="1d",
            start_time=start_date
        ).to_dataframe()
        # Assign datatypes
        df_prices["time"] = pd.to_datetime(df_prices.time)
        # Convert time to string
        df_prices["time"] = df_prices["time"].dt.strftime("%Y-%m-%d")
        df_prices["Price"] = df_prices.PriceUSD.astype(float)
        # Reshape dataset so assets are in columns, dates are the rows, and the values are prices
        df_prices_pivot = df_prices.pivot(
            index="time",
            columns="asset",
            values="PriceUSD"
        )
        # Calculate compound interest
        df = Compound(df_prices_pivot, int(amount), period, asset)

        return json.dumps(df["Compound"].to_dict())
    else:
        return "Asset not found"

# create a route to get the compound interest for a specific asset and for a specific range of date
@app.route('/compound/<asset>/<amount>/<period>/<start_date>/<end_date>')
def get_compound_date(asset, amount, period, start_date, end_date):
    """
            Returns a list of the compound interests you could have earned with <amount> $ spent each <period> in <asset> from <start_date> to <end_date>
            asset: the asset you want to invest in ( go to /assets to look at the list of assets )
            amount: the amount you want to invest each period ( int )
            period: the period you want to invest for ( "d", "w", "y" )
            start_date: the start date of the range you want to get compound interest for
            end_date: the end date of the range you want to get compound interest for
            ---
            responses:
              200:
                description: Great success!
            """
    assets = client.catalog_metrics()[0]["frequencies"][0]["assets"]
    if asset in assets and period in ["d", "m", "y"]:
        df_prices = client.get_asset_metrics(
            assets=[asset],
            metrics="PriceUSD",
            frequency="1d",
            start_time=start_date,
            end_time=end_date
        ).to_dataframe()
        # Assign datatypes
        df_prices["time"] = pd.to_datetime(df_prices.time)
        # Convert time to string
        df_prices["time"] = df_prices["time"].dt.strftime("%Y-%m-%d")
        df_prices["Price"] = df_prices.PriceUSD.astype(float)
        # Reshape dataset so assets are in columns, dates are the rows, and the values are prices
        df_prices_pivot = df_prices.pivot(
            index="time",
            columns="asset",
            values="PriceUSD"
        )
        # Calculate compound interest
        df = Compound(df_prices_pivot, int(amount), period, asset)

        return json.dumps(df["Compound"].to_dict())
    else:
        return "Asset not found"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
