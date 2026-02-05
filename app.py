from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import calendar

import plotly.express as px
from shiny import reactive
from shiny.express import render, input, ui
from shinywidgets import render_plotly

ui.tags.style(
    """
        .header-container {
            display: flex;
            align-items: center;
            height: 60px;
            background-color: #5DADE2;
        }

        .logo-container {
            margin-right: 5px;
            height: 100% !important;
            padding: 10px;
        }
        .logo-container img {
            height: 40px;
        }

        .title-container h2 {
            color: white;
            padding: 10px;
            border-radius: 5px;
            margin: 0;
        }
    """
)

ui.page_opts(window_title="Sales Dashboard", fillable=False)

# schedule = pd.read_parquet('./schedule.parquet')
# window = pd.read_parquet('./window.parquet')

