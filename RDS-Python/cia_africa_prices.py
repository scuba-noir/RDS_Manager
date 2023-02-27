import pymysql 
import pandas as pd
import numpy as np
import datetime
import snscrape.modules.twitter as sntwitter
import pandas as pd
import numpy as np
from textblob import TextBlob
import re
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import pathlib as Path

from openpyxl import load_workbook

import warnings
warnings.filterwarnings('ignore')

def upload_new_prices():

    base_dir = "C:\\Users\ChristopherTHOMPSON\OneDrive - AlmaStone\AlmaStone\Platform\Technology and Systems\Agrosystems\Market Prices\CIA_Attachments(Do Not Touch)\\"
    xlsx_ls = os.listdir("C:\\Users\\ChristopherTHOMPSON\\AlmaStone\\Hub - Team\\AlmaStone\\Platform\\Technology and Systems\\Agrosystems\\Market Prices\\CIA_Attachments(Do Not Touch)")
    output_df = pd.DataFrame()
    counter = 0
    for file in xlsx_ls:
        temp_df = pd.read_excel(base_dir + '\\' + file, index_col=False)
        if counter == 0:
            output_df = temp_df
        else:
            output_df = pd.concat([output_df, temp_df], axis = 0, ignore_index=True)
        counter += 1
    
    return output_df.drop_duplicates()


x = upload_new_prices()
x.to_excel('test_ciaf_prices.xlsx', index = False)