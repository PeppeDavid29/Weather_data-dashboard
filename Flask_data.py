import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import nest_asyncio


nest_asyncio.apply()


csv_file_path = r"C:\Users\sharo\ordered_by_city.csv"
weather_data = pd.read_csv(csv_file_path)
state_coordinates = {
    'Kerala': [10.8505, 76.2711], 'Andhra Pradesh': [15.9129, 79.7400], 'Arunachal Pradesh': [28.2180, 94.7278],
    'Assam': [26.2006, 92.9376], 'Bihar': [25.0961, 85.3131], 'Chhattisgarh': [21.2787, 81.8661],
    'Goa': [15.2993, 74.1240], 'Gujarat': [22.2587, 71.1924], 'Haryana': [29.0588, 76.0856],
    'Himachal Pradesh': [31.1048, 77.1734], 'Jharkhand': [23.6102, 85.2799], 'Karnataka': [15.3173, 75.7139],
    'Madhya Pradesh': [22.9734, 78.6569], 'Maharashtra': [19.7515, 75.7139], 'Manipur': [24.6637, 93.9063],
    'Meghalaya': [25.4670, 91.3662], 'Mizoram': [23.1645, 92.9376], 'Nagaland': [26.1584, 94.5624],
    'Odisha': [20.9517, 85.0985], 'Punjab': [31.1471, 75.3412], 'Rajasthan': [27.0238, 74.2179],
    'Sikkim': [27.5330, 88.5122], 'Tamil Nadu': [11.1271, 78.6569], 'Hyderabad': [17.3850, 78.4867],
    'Tripura': [23.9408, 91.9882], 'Uttar Pradesh': [26.8467, 80.9462]
}

weather_data['latitude'] = weather_data['location'].map(lambda loc: state_coordinates.get(loc, [None, None])[0])
weather_data['longitude'] = weather_data['location'].map(lambda loc: state_coordinates.get(loc, [None, None])[1])


app = dash.Dash(__name__)


app.layout = html.Div([
    html.H1("Weather Data Visualization Across India"),
    
    dcc.Dropdown(
        id='parameter-dropdown',
        options=[
            {'label': 'Temperature', 'value': 'temperature'},
            {'label': 'Humidity', 'value': 'humidity'},
            {'label': 'Wind Speed', 'value': 'wind_speed'}
        ],
        value='temperature'
    ),
    
    dcc.Graph(id='map-graph')
])


@app.callback(
    Output('map-graph', 'figure'),
    [Input('parameter-dropdown', 'value')]
)
def update_map(selected_parameter):
    fig = px.scatter_geo(
        weather_data,
        lat='latitude',
        lon='longitude',
        color=selected_parameter,
        hover_name='location',
        hover_data={'latitude': False, 'longitude': False},
        projection='natural earth',
        title=f'{selected_parameter.capitalize()} across India'
    )
    
    fig.update_geos(
        showcountries=True, countrycolor="Black",
        showsubunits=True, subunitcolor="Blue",
        scope='asia',
        fitbounds="locations"
    )
    
    fig.update_layout(
        geo=dict(
            center=dict(lat=22.3511148, lon=78.6677428),
            lataxis_range=[6, 38],
            lonaxis_range=[68, 97]
        )
    )
    
    return fig

if __name__ == '__main__':
    app.run_server(host='127.0.0.1', port=8050)