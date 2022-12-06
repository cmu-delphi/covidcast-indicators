from datetime import date
import pandas as pd

#Regions considered under states
STATES = ['ak', 'al', 'ar', 'as', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga',
            'gu', 'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la',
            'ma', 'md', 'me', 'mi', 'mn', 'mo', 'mp', 'ms', 'mt', 'nc',
            'nd', 'ne', 'nh', 'nj', 'nm', 'nv', 'ny', 'oh', 'ok',
            'or', 'pa', 'pr', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'va', 'vi', 'vt',
            'wa', 'wi', 'wv', 'wy', 'us']

HOLIDAYS = pd.to_datetime(["1/1/2020","1/20/2020","2/17/2020","5/25/2020","7/3/2020","9/7/2020","10/12/2020","11/11/2020","11/26/2020","12/25/2020",
"1/1/2021","1/18/2021","2/15/2021","5/31/2021","6/18/2021","7/05/2021","9/06/2021","10/11/2021","11/11/2021","11/25/2021","12/24/2021","12/31/2021",
"1/17/2022","2/21/2022","5/30/2022","6/20/2022","7/04/2022","9/05/2022","10/10/2022","11/11/2022","11/24/2022","12/26/2022"])


#HTML Link for the visualization tool alerts
HTML_LINK = "<https://ananya-joshi-visapp-vis-523f3g.streamlitapp.com/?params="