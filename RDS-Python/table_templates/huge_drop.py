"""
    sql = '''drop table auth_group_permissions'''
    cursor.execute(sql)
    
    sql = '''drop table auth_group'''
    cursor.execute(sql)

    sql = '''drop table auth_permission'''
    cursor.execute(sql)

    sql = '''drop table auth_user'''
    cursor.execute(sql)

    sql = '''drop table auth_user_groups'''
    cursor.execute(sql)

    sql = '''drop table auth_user_user_permissions'''
    cursor.execute(sql)
    
    sql = '''drop table django_admin_log'''
    cursor.execute(sql)
    
    sql = '''drop table django_content_type'''
    cursor.execute(sql)

    sql = '''drop table django_migrations'''
    cursor.execute(sql)
    sql = '''drop table django_session'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_financial_simulation_meta_data_historical'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_financial_simulation_results'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_forecast_assumptions_user_fin_sim'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_mc_results_meta_vars'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_monte_carlo_longform_results'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_quarterly_forecast'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_quarterly_forecast_table'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_simulation_drivers'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_var_table'''
    cursor.execute(sql)
    sql = '''drop table web_app_1_var_table_entry'''
    cursor.execute(sql)
    """