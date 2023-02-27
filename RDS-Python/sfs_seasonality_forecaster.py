import pandas as pd
import numpy as np
import pdblp
import matplotlib.pyplot as plt
import datetime
import math

from prophet import Prophet
from datetime import date

#from sktime.forecasting.arima import AutoARIMA
import warnings
warnings.filterwarnings("ignore")

def remaining_season(ds):
    date = pd.to_datetime(ds)
    today = date.today()
    year = today.year
    if today.month < 8:
        year = today.year + 1

    return ((date.month > today.month & date.year == year) | (date.month < today.month & date.year < year))

def runSim():

    con = pdblp.BCon(debug=False, port=8194, timeout=5000)
    con.start()
    today = date.today().strftime('%Y%m%d')
    end_sim_date = date(2023, 8, 31)
    duration = pd.date_range(start=date.today(), end=end_sim_date, freq='D')
    data_df = con.bdh(['UKAGSSUT Index', 'UAHUSD Curncy'], 'PX_LAST', start_date='20000101', end_date=today, ovrds=[('Period','D')], longdata=False).dropna()
    data_df.columns = ['UKAGSSUT Index', 'UAHUSD Curncy']
    data_df = data_df.reset_index(names = 'ds')
    holiday_df = pd.DataFrame({
            'holiday': 'new_span',
            'ds': pd.date_range(start='8/31/2000', end='8/31/2023', freq='Y'),
            'lower_window': 0,
            'upper_window': 365
        })
    data_df['y'] = data_df['UKAGSSUT Index'] * data_df['UAHUSD Curncy']
    final_df = data_df.drop(['UKAGSSUT Index', 'UAHUSD Curncy'], axis = 1)
    
    #print(final_df)
    model = Prophet(daily_seasonality=False, weekly_seasonality=False, yearly_seasonality=True, changepoint_prior_scale=0.80)
    #model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
    model.fit(final_df)
    px_forecast = model.make_future_dataframe(periods = duration.size, freq = 'D', include_history=False)
    px_forecast = model.predict(px_forecast)
    plt.figure()
    model.plot(px_forecast)
    model.plot_components(px_forecast, yearly_start=datetime.datetime.now().timetuple().tm_yday)
    plt.show()
    px_forecast.to_excel('sfs_sesasonality_output_5.xlsx', index=False)
    con.stop()

    """
    test_len = int(len(final_df) * 0.3)
    al_train, al_test = final_df.iloc[:-test_len], final_df.iloc[-test_len:]

    forecaster = AutoARIMA(sp=12, suppress_warnings=True)
    forecaster.fit(al_train)
    forecaster.summary()
    fh = np.arange(test_len) + 1
    forecast, forecast_int = forecaster.predict(fh=fh, return_pred_int=True, alpha=0.05)
    al_arima_mae, al_arima_mape = plot_forecast(al_train, al_test, forecast, forecast_int)
    """

def runsim_2():

    con = pdblp.BCon(debug=False, port=8194, timeout=5000)
    con.start()
    today = date.today().strftime('%Y%m%d')
    end_sim_date = date(2023, 8, 31)
    sim_start = date(year = date.today().year - 6, month = date.today().month, day = date.today().day).strftime('%Y%m%d')
    data_df = con.bdh(['UKAGSSUT Index', 'UAHUSD Curncy'], 'PX_LAST', start_date=sim_start, end_date=today, ovrds=[('Period','D')], longdata=False).dropna()
    data_df.columns = ['UKAGSSUT Index', 'UAHUSD Curncy']
    #data_df = data_df.reset_index(names = 'ds')
    data_df['y'] = data_df['UKAGSSUT Index'] * data_df['UAHUSD Curncy']
    final_df = data_df.drop(['UKAGSSUT Index', 'UAHUSD Curncy'], axis = 1)
    start_price = final_df['y'].iat[-1]
    final_df = final_df.resample('W-FRI').ffill().reset_index(names='ds')
    pct_change = final_df['y'].pct_change(-1).dropna()
    mu = pct_change.mean()
    sigma = math.sqrt(pct_change.std())
    new_df = []
    spot_df = []
    counter = 0
    for i in range(end_sim_date.year - final_df['ds'].min().year):
        temp_year = end_sim_date.year - i
        date_0 = pd.to_datetime(date(temp_year-1, date.today().month, date.today().day))
        date_n = pd.to_datetime(date(temp_year, 8, 31))
        temp_df = final_df['y'].loc[(final_df['ds'] <= date_n) & (final_df['ds'] >= date_0)].T
        temp_col = temp_df.reset_index(drop=True)
        if temp_year > date.today().year + 1:
            continue
        if temp_year == date.today().year:
            spot_df = pd.DataFrame(temp_col.values, columns=[temp_year])
        else:
            if counter == 0:
                new_df = pd.DataFrame(temp_col.values, columns=[temp_year])
                counter += 1
            else:
                new_df = pd.concat([new_df, pd.DataFrame(temp_col.values, columns=[temp_year])], axis =1)

    pct_change_hist = new_df.pct_change().fillna(0)
    pct_change_hist = pct_change_hist + 1
    avg_df = []
    final_values_df = [start_price]
    upper_values = [start_price]
    lower_values = [start_price]

    mc_sim = []
    for q in range(pct_change.shape[0]):
        mc_sim.append(np.random.normal(loc = mu, scale = sigma, size = 10000).tolist())

    mc_sim = pd.DataFrame(mc_sim)
    mc_sim = (1+mc_sim).cumprod()*start_price

    print(mc_sim)
    indexer = 0
    for rows, values in pct_change_hist.iterrows():
        avg_df.append(values.mean())
        final_values_df.append(final_values_df[indexer] * values.mean())
        upper_values.append(np.percentile(mc_sim[indexer], 95))
        lower_values.append(np.percentile(mc_sim[indexer], 5))
        indexer += 1


    avg_df = pd.DataFrame(avg_df, columns = ['mean_chg'])
    duration = pd.date_range(start=date.today(),freq='W-FRI', periods=len(final_values_df))
    final_values_df = pd.DataFrame(final_values_df, columns = ['calc_values']).set_index(duration).reset_index(names=['date'])
    final_values_df['upper_values'] = upper_values
    final_values_df['lower_values'] = lower_values
    final_values_df.to_excel('final_values_output_2.xlsx', index=False)
    print(final_values_df)


runsim_2()